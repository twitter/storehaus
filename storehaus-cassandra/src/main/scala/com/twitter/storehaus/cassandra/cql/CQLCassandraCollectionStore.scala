/*
 * Copyright 2014 SEEBURGER AG
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
import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet, Row, SimpleStatement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{QueryBuilder, BuiltStatement, Update, Delete, Select}
import com.datastax.driver.core.DataType.Name
import com.twitter.storehaus.{IterableStore, Store}
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.WithPutTtl
import com.datastax.driver.core.querybuilder.Insert
import java.util.concurrent.Executors
import java.util.ArrayList
import scala.collection.immutable.Nil
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import shapeless._
import HList._
import ops.hlist.Mapper
import ops.hlist.Mapped
import ops.hlist.ToList
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
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  tablecomment: Option[String] = None)
	(implicit mrk: Mapper.Aux[keyStringMapping.type, RS, MRKResult],
       mck: Mapper.Aux[keyStringMapping.type, CS, MCKResult],
       tork: ToList[MRKResult, String],
       tock: ToList[MCKResult, String],
       ev0: ¬¬[V] <:< (Set[X] ∨ List[X]),
	   ev1: CassandraPrimitive[X])= {
    createColumnFamilyWithToken[RS, CS, V, X, MRKResult, MCKResult, String] (columnFamily, rowkeySerializers, 
        rowkeyColumnNames, colkeySerializers, colkeyColumnNames, valueSerializer, traversableType, 
        None, "", valueColumnName, tablecomment)
  }

  def createColumnFamilyWithToken[RS <: HList, CS <: HList, V, X, MRKResult <: HList, MCKResult <: HList, T] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializer: CassandraPrimitive[X],
	traversableType: V,
	tokenSerializer: Option[CassandraPrimitive[T]] = None,
	tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  tablecomment: Option[String] = None)
	(implicit mrk: Mapper.Aux[keyStringMapping.type, RS, MRKResult],
       mck: Mapper.Aux[keyStringMapping.type, CS, MCKResult],
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
	        (tokenSerializer match {
	        	case Some(tokSer) => ", \"" + tokenColumnName + "\" " + tokSer.cassandraType + ", "
	        	case _ => ""
    		}) +
	        "PRIMARY KEY ((\"" + rowkeyColumnNames.mkString("\", \"") + "\"), \"" +
	        colkeyColumnNames.mkString("\", \"") + "\"))" + 
          tablecomment.map(comment => s" WITH comment='$comment';").getOrElse(";")
    columnFamily.session.getSession.execute(stmt)
  } 
}

/**
 * The resulting type V is List[X] v Set[X] 
 */
class CQLCassandraCollectionStore[RK <: HList, CK <: HList, V, X, RS <: HList, CS <: HList] (
  override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  val rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  val colkeyColumnNames: List[String],
  val valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
    (mergeSemigroup: Semigroup[V],
     sync: CassandraExternalSync = CQLCassandraConfiguration.DEFAULT_SYNC)
  	(implicit evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
  			evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
		    rowmap: AbstractCQLCassandraCompositeStore.Row2Result[RK, RS],
		    colmap: AbstractCQLCassandraCompositeStore.Row2Result[CK, CS],
  			a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
  			a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
  			rsUTC: *->*[CassandraPrimitive]#λ[RS],
  			csUTC: *->*[CassandraPrimitive]#λ[CS],
  			ev0: ¬¬[V] <:< (Set[X] ∨ List[X]),
		    ev2: CassandraPrimitive[X]) 
	extends AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS](columnFamily, rowkeySerializer, 
	    rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency,
	    poolSize, batchType, ttl)
		    with MergeableStore[(RK, CK), V] 
            with WithPutTtl[(RK, CK), V, CQLCassandraCollectionStore[RK, CK, V, X, RS, CS]] 
            with CassandraCASStore[(RK, CK), V] {
  
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
    val lockId = mapKeyToSyncId((rk, ck), columnFamily)
    futurePool {
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer)
      addKey(ck, colkeyColumnNames, eqList, colkeySerializer)
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
      }.flatMap(stmt => sync.merge.lock(lockId, Future {
   	      val origValue = Await.result(get((rk, ck)))
	      // due to some unknown reason stmt does not execute in the same session object, except when executed multiple times, so we execute in a different one
   	      // TODO: this must be removed in the future as soon as the bug (?) is fixed in Datastax' driver 
   	      val newSession = columnFamily.session.cluster.getCluster.connect("\"" + columnFamily.session.keyspacename + "\"") 
   	      newSession.execute(stmt)
   	      newSession.close()
          origValue
        })
      )
  }
  
  override def getRowValue(row: Row): V = {
    val mainDataType = row.getColumnDefinitions().getType(valueColumnName)
    val optResult = mainDataType.getName() match {
      case Name.SET => Some(row.getSet(valueColumnName, ev2.cls).asScala.
            map(e => ev2.fromCType(e.asInstanceOf[AnyRef])).toSet.asInstanceOf[V])
      case Name.LIST => Some(row.getList(valueColumnName, ev2.cls).asScala.
            map(e => ev2.fromCType(e.asInstanceOf[AnyRef])).toList.asInstanceOf[V])
      case _ => None
    }
    optResult.get
  }
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, (RK, CK), V] with IterableStore[(RK, CK), V] = new CQLCassandraCollectionStore[RK, CK, V, X, RS, CS](
      columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType,
      ttl)(mergeSemigroup, sync) with CassandraCASStoreSimple[T, (RK, CK), V] {
    override protected def putValue(value: V, update: Update): Update.Assignments = super.putValue(value, update).and(QueryBuilder.set(tokenColumnName, tokenFactory.createNewToken))
    override protected def deleteColumns: String = s"$valueColumnName , $tokenColumnName"
    override def cas(token: Option[T], kv: ((RK, CK), V))(implicit ev1: Equiv[T]): Future[Boolean] =
      casImpl(token, kv, createPutQuery[(RK, CK)](_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    override def get(key: (RK, CK))(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = 
      getImpl(key, createGetQuery(_), cassTokenSerializer, getRowValue(_), tokenColumnName, columnFamily, consistency)(ev1)
  }
}
