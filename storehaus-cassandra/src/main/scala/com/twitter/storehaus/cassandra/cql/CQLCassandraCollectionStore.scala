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
import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet, SimpleStatement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{QueryBuilder, BuiltStatement, Update, Delete, Select}
import com.datastax.driver.core.DataType.Name
import com.twitter.storehaus.Store
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.WithPutTtl
import com.datastax.driver.core.querybuilder.Insert
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
import com.datastax.driver.core.SimpleStatement

object CQLCassandraCollectionStore {
  import AbstractCQLCassandraCompositeStore._
  
  /**
   * Optionally this method can be used to setup the column family on the Cassandra cluster.
   * Implicits are from shapeless. Types in the set/list must be CassandraPrimitive types.
   */
  def createColumnFamily[RS <: HList, CS <: HList, V, X, MRKResult <: HList, MCKResult <: HList] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializer: CassandraPrimitive[X],
	traversableType: V,
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME)
	(implicit mrk: MapperAux[keyStringMapping.type, RS, MRKResult],
       mck: MapperAux[keyStringMapping.type, CS, MCKResult],
       tork: ToList[MRKResult, String],
       tock: ToList[MCKResult, String],
       ev0: ¬¬[V] <:< (Set[X] ∨ List[X]),
	   ev1: CassandraPrimitive[X])= {
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
	        (traversableType match {
	        	case set: Set[_] => createColumnListing(List(valueColumnName), List("set<" + valueSerializer.cassandraType + ">")) 
	        	case list: List[_] => createColumnListing(List(valueColumnName), List("list<" + valueSerializer.cassandraType + ">")) 
	        	case _ => ""
    		}) +
	        "PRIMARY KEY ((\"" + rowkeyColumnNames.mkString("\", \"") + "\"), \"" +
	        colkeyColumnNames.mkString("\", \"") + "\"));"
    columnFamily.session.getSession.execute(stmt)
  } 
}

/**
 * The resulting type V is List[X] v Set[X] 
 */
class CQLCassandraCollectionStore[RK <: HList, CK <: HList, V, X, RS <: HList, CS <: HList] (
  columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
    (mergeSemigroup: Semigroup[V],
     sync: CassandraExternalSync = CQLCassandraConfiguration.DEFAULT_SYNC)
  	(implicit evrow: MappedAux[RK, CassandraPrimitive, RS],
  			evcol: MappedAux[CK, CassandraPrimitive, CS], 
  			a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK], 
  			a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK],
  			rsUTC: *->*[CassandraPrimitive]#λ[RS],
  			csUTC: *->*[CassandraPrimitive]#λ[CS],
  			ev0: ¬¬[V] <:< (Set[X] ∨ List[X]),
		    ev2: CassandraPrimitive[X]) 
	extends AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS](columnFamily, rowkeySerializer, 
	    rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency,
	    poolSize, batchType, ttl)
		    with MergeableStore[(RK, CK), V] with WithPutTtl[(RK, CK), V, CQLCassandraCollectionStore[RK, CK, V, X, RS, CS]] {
  
  override def withPutTtl(ttl: Duration): CQLCassandraCollectionStore[RK, CK, V, X, RS, CS] = new CQLCassandraCollectionStore[RK, CK, V, X, RS, CS](
    columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType,
    Option(ttl))(mergeSemigroup, sync)

  
  override protected def putValue(value: V, update: Update): Update.Assignments = {
    value match {
      case set: Set[X] => update.`with`(QueryBuilder.set(valueColumnName, set.map(v => ev2.toCType(v)).toSet.asJava))
      case list: List[X] => update.`with`(QueryBuilder.set(valueColumnName, list.map(v => ev2.toCType(v)).toList.asJava))
    }
  }

  override protected def getValue(result: ResultSet): Option[V] = { 
    val row = result.one()
    val mainDataType = row.getColumnDefinitions().getType(valueColumnName)
    mainDataType.getName() match {
      case Name.SET => Some(row.getSet(valueColumnName, ev2.cls).asScala.
            map(e => ev2.fromCType(e.asInstanceOf[AnyRef])).toSet.asInstanceOf[V])
      case Name.LIST => Some(row.getList(valueColumnName, ev2.cls).asScala.
            map(e => ev2.fromCType(e.asInstanceOf[AnyRef])).toList.asInstanceOf[V])
      case _ => None
    }
  }
  
  override def semigroup: Semigroup[V] = mergeSemigroup
  
  /**
   * to retrieve an old value, this needs synchronization primitives (see constructor) otherwise you can use NoSync
   */
  override def merge(kv: ((RK, CK), V)): Future[Option[V]] = {
    import AbstractCQLCassandraCompositeStore._
    val ((rk, ck), value) = kv
    futurePool {
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList)
      addKey(ck, colkeyColumnNames, eqList)
      val update = QueryBuilder.update(columnFamily.getPreparedNamed)
      val initialsetfunc = (value match {
          case set: Set[X] => update.`with`(QueryBuilder.addAll(valueColumnName, set.map(v => ev2.toCType(v)).toSet.asJava))
          case list: List[X] => update.`with`(QueryBuilder.appendAll(valueColumnName, list.map(v => ev2.toCType(v)).toList.asJava))
        }).where(_)
      val where = eqList.join(initialsetfunc)((clause, where) => where.and(clause))
      val stmt: BuiltStatement = (ttl match {
          case Some(duration) => where.using(QueryBuilder.ttl(duration.inSeconds))
          case None => where
        })
      stmt.setConsistencyLevel(consistency)
      val lockId = mapKeyToSyncId((rk, ck), columnFamily)
      Await.result(sync.merge.lock(lockId, Future {
   	    val origValue = Await.result(get((rk, ck)))
	    // due to some unknown reason stmt does not execute in the same session object, except when executed multiple times, so we execute in a different one
   	    // TODO: this must be removed in the future as soon as the bug (?) is fixed in Datastax' driver 
   	    val newSession = columnFamily.session.cluster.getCluster.connect("\"" + columnFamily.session.keyspacename + "\"") 
   	    newSession.execute(stmt)
   	    newSession.close()
        origValue
      }))
    }
  }
}

