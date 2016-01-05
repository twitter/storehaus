/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet, Row}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Insert, BuiltStatement, Update, Delete, Select}
import com.datastax.driver.core.DataType.Name
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{WithPutTtl, WritableStore}
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{Await, Future, Duration, FuturePool}
import com.websudos.phantom.CassandraPrimitive
import scala.language.implicitConversions
import java.util.ArrayList
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import shapeless.HList
import shapeless.ops.hlist.{Mapped, Mapper, ToList}


object CQLCassandraLongStore {
  import AbstractCQLCassandraCompositeStore._
  
  /**
   * Optionally this method can be used to setup the column family on the Cassandra cluster.
   * Implicits can be imported from shapeless to use this.
   */
  def createColumnFamily[RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME)
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
      cs2str: CassandraPrimitivesToStringlist[CS])= {
      def createColumnListing(names: List[String], types: List[String]): String = names.size match {
          case 0 => ""
          case _ => "\"" + names.head.filterNot(_ == '"') + "\" " + types.head.filterNot(_ == '"') + ", " + createColumnListing(names.tail, types.tail)
      }
    columnFamily.session.createKeyspace
    val rowKeyStrings = rowkeySerializers.stringlistify
    val colKeyStrings = colkeySerializers.stringlistify
    val stmt = s"""CREATE TABLE IF NOT EXISTS ${columnFamily.getPreparedNamed} (""" +
    		createColumnListing(rowkeyColumnNames, rowKeyStrings) +
	        createColumnListing(colkeyColumnNames, colKeyStrings) +
	        createColumnListing(List(valueColumnName), List("counter")) +
	        "PRIMARY KEY ((\"" + rowkeyColumnNames.mkString("\", \"") + "\"), \"" +
	        colkeyColumnNames.mkString("\", \"") + "\"));"
    columnFamily.session.getSession.execute(stmt)
  } 
}


/**
 * store that can _merge_ Longs
 */
class CQLCassandraLongStore[RK <: HList, CK <: HList, RS <: HList, CS <: HList] (
  override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE)
    (readbeforeWrite: Boolean = true,
     sync: CassandraExternalSync = CQLCassandraConfiguration.DEFAULT_SYNC)
  	(implicit evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
  			evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
		    rowmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[RS, RK],
		    colmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[CS, CK],
  			a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
  			a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
		    ev2: CassandraPrimitive[Long]) 
	extends AbstractCQLCassandraCompositeStore[RK, CK, Long, RS, CS](columnFamily, rowkeySerializer, 
	    rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency,
	    poolSize, batchType, None)
		    with MergeableStore[(RK, CK), Long] with WritableStore[(RK, CK), Option[Long]] { 
  
  override def multiPut[K1 <: (RK, CK)](kvs: Map[K1, Option[Long]]): Map[K1, Future[Unit]] = super[WritableStore].multiPut(kvs)

  override protected def putValue[S <: BuiltStatement, U <: BuiltStatement, T](value: Long, stmt: S, token: Option[T]): U = stmt match {
    case update: Update => update.`with`(QueryBuilder.incr(valueColumnName, 
        implicitly[CassandraPrimitive[Long]].toCType(value).asInstanceOf[java.lang.Long])).asInstanceOf[U]
  }
  
  override def put(kv: ((RK, CK), Option[Long])): Future[Unit] = {
    import AbstractCQLCassandraCompositeStore._
    val ((rk, ck), valueOpt) = kv
    val lockId = mapKeyToSyncId((rk, ck), columnFamily)
    // syncs won't work well with batching so we provide put, not multiPut  
    futurePool {
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer)
      addKey(ck, colkeyColumnNames, eqList, colkeySerializer)
      eqList
    }.flatMap(eqList => sync.put.lock(lockId, {
        val origValue = if(readbeforeWrite) Await.result(get((rk, ck))).getOrElse(0l) else 0l
    	val value = valueOpt.getOrElse(0l)
    	val update = putValue[Update, Update.Assignments, Boolean](value - origValue, QueryBuilder.update(columnFamily.getPreparedNamed), Some(true)).where(_)
    	val stmt = eqList.join(update)((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
    	columnFamily.session.getSession.execute(stmt)
    	if (valueOpt.isEmpty) {
    	  // delete value (Cassandra's way to handle deletion of counters can still result in re-appearance, but at least we have set it to 0)
    	  val delete = eqList.join(QueryBuilder.delete(valueColumnName).from(columnFamily.getPreparedNamed).where(_))((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
    	  columnFamily.session.getSession.execute(delete)
    	}
      })
    )
  }
  
  override protected def getValue(result: ResultSet): Option[Long] = Some(result.one().getLong(valueColumnName).asInstanceOf[Long])
  
  override def semigroup: Semigroup[Long] = implicitly[Semigroup[Long]]
  
  /**
   * to retrieve an old value, this needs synchronization primitives (see constructor) otherwise you can use NoSync
   */
  override def merge(kv: ((RK, CK), Long)): Future[Option[Long]] = {
    import AbstractCQLCassandraCompositeStore._
    val ((rk, ck), value) = kv
    val lockId = mapKeyToSyncId((rk, ck), columnFamily)
    futurePool {
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer)
      addKey(ck, colkeyColumnNames, eqList, colkeySerializer)
      val update = QueryBuilder.update(columnFamily.getPreparedNamed)
      eqList.join{
          (if(value < 0) update.`with`(QueryBuilder.decr(valueColumnName, implicitly[CassandraPrimitive[Long]].toCType(-1 * value).asInstanceOf[java.lang.Long]))
          else update.`with`(QueryBuilder.incr(valueColumnName, implicitly[CassandraPrimitive[Long]].toCType(value).asInstanceOf[java.lang.Long]))
        ).where(_)}((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
    }.flatMap(stmt => sync.merge.lock(lockId, {
    	  val origValue = if(readbeforeWrite) Await.result(get((rk, ck))) else Some(0l)
          columnFamily.session.getSession.execute(stmt)
          origValue
    }))
  }
  
  override def getRowValue(row: Row): Long = implicitly[CassandraPrimitive[Long]].fromRow(row, valueColumnName).get
}  

