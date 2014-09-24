/*
 * Copyright 2014 Twitter Inc.
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

import com.websudos.phantom.CassandraPrimitive
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{Await, Future, Duration, FuturePool}
import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{QueryBuilder, BuiltStatement, Update, Delete, Select}
import com.datastax.driver.core.DataType.Name
import com.twitter.storehaus.WritableStore
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.WithPutTtl
import com.datastax.driver.core.querybuilder.Insert
import scala.language.implicitConversions
import java.util.concurrent.Executors
import java.util.ArrayList
import scala.collection.immutable.Nil
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import shapeless.TypeOperators._
import shapeless._
import HList._
import Traversables._
import Nat._
import UnaryTCConstraint._
import scala.collection.mutable.ArrayBuffer
import com.datastax.driver.core.querybuilder.Clause

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
	(implicit mrk: MapperAux[keyStringMapping.type, RS, MRKResult],
       mck: MapperAux[keyStringMapping.type, CS, MCKResult],
       tork: ToList[MRKResult, String],
       tock: ToList[MCKResult, String])= {
      def createColumnListing(names: List[String], types: List[String]): String = names.size match {
          case 0 => ""
          case _ => "\"" + names.head.filterNot(_ == '"') + "\" " + types.head.filterNot(_ == '"') + ", " + createColumnListing(names.tail, types.tail)
      }
    columnFamily.session.createKeyspace
    val rowKeyStrings = rowkeySerializers.map(keyStringMapping).toList
    val colKeyStrings = colkeySerializers.map(keyStringMapping).toList
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
  columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE)
    (readbeforeWrite: Boolean = true,
     sync: CassandraExternalSync = CQLCassandraConfiguration.DEFAULT_SYNC)
  	(implicit evrow: MappedAux[RK, CassandraPrimitive, RS],
  			evcol: MappedAux[CK, CassandraPrimitive, CS], 
  			a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK], 
  			a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK],
  			rsUTC: *->*[CassandraPrimitive]#λ[RS],
  			csUTC: *->*[CassandraPrimitive]#λ[CS],
		    ev2: CassandraPrimitive[Long]) 
	extends AbstractCQLCassandraCompositeStore[RK, CK, Long, RS, CS](columnFamily, rowkeySerializer, 
	    rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency,
	    poolSize, batchType, None)
		    with MergeableStore[(RK, CK), Long] with WritableStore[(RK, CK), Option[Long]] { 
  
  override def multiPut[K1 <: (RK, CK)](kvs: Map[K1, Option[Long]]): Map[K1, Future[Unit]] = super[WritableStore].multiPut(kvs)

  override protected def putValue(value: Long, update: Update): Update.Assignments = {
    update.`with`(QueryBuilder.incr(valueColumnName, implicitly[CassandraPrimitive[Long]].toCType(value).asInstanceOf[java.lang.Long]))
  }
  
  override def put(kv: ((RK, CK), Option[Long])): Future[Unit] = {
    import AbstractCQLCassandraCompositeStore._
    // syncs won't work well with batching so we provide put, not multiPut  
    futurePool {
   	  val ((rk, ck), valueOpt) = kv
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList)
      addKey(ck, colkeyColumnNames, eqList)
      val lockId = mapKeyToSyncId((rk, ck), columnFamily)
      Await.result(sync.put.lock(lockId, Future {
        val origValue = if(readbeforeWrite) Await.result(get((rk, ck))).getOrElse(0l) else 0l
    	val value = valueOpt.getOrElse(0l)
    	val update = putValue(value - origValue, QueryBuilder.update(columnFamily.getPreparedNamed)).where(_)
    	val stmt = eqList.join(update)((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
    	columnFamily.session.getSession.execute(stmt)
    	if (valueOpt.isEmpty) {
    	  // delete value (Cassandra's way to handle deletion of counters can still result in re-appearance, but at least we have set it to 0)
    	  val delete = eqList.join(QueryBuilder.delete(valueColumnName).from(columnFamily.getPreparedNamed).where(_))((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
    	  columnFamily.session.getSession.execute(delete)
    	}
      }))
    } 
  }
  
  override protected def getValue(result: ResultSet): Option[Long] = Some(result.one().getLong(valueColumnName).asInstanceOf[Long])
  
  override def semigroup: Semigroup[Long] = implicitly[Semigroup[Long]]
  
  /**
   * to retrieve an old value, this needs synchronization primitives (see constructor) otherwise you can use NoSync
   */
  override def merge(kv: ((RK, CK), Long)): Future[Option[Long]] = {
    import AbstractCQLCassandraCompositeStore._
    val ((rk, ck), value) = kv
    futurePool {
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList)
      addKey(ck, colkeyColumnNames, eqList)
      val update = QueryBuilder.update(columnFamily.getPreparedNamed)
      val stmt = eqList.join{
          (if(value < 0) update.`with`(QueryBuilder.decr(valueColumnName, implicitly[CassandraPrimitive[Long]].toCType(-1 * value).asInstanceOf[java.lang.Long]))
          else update.`with`(QueryBuilder.incr(valueColumnName, implicitly[CassandraPrimitive[Long]].toCType(value).asInstanceOf[java.lang.Long]))
        ).where(_)}((clause, where) => where.and(clause)).setConsistencyLevel(consistency)
      val lockId = mapKeyToSyncId((rk, ck), columnFamily)
      Await.result(sync.merge.lock(lockId, Future {
    	  val origValue = if(readbeforeWrite) Await.result(get((rk, ck))) else Some(0l)
          columnFamily.session.getSession.execute(stmt)
          origValue
      }))
    }
  }
}  

