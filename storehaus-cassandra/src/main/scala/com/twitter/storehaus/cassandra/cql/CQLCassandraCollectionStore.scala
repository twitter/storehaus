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

import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet, Row, SimpleStatement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, BuiltStatement, Insert, Update, Delete, Select}
import com.datastax.driver.core.DataType.Name
import com.websudos.phantom.CassandraPrimitive
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{IterableStore, Store, WithPutTtl}
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{Await, Closable, Future, Duration, FuturePool}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import shapeless._
import shapeless.ops.hlist.{Mapped, Mapper, ToList}

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
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
      cs2str: CassandraPrimitivesToStringlist[CS],
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
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
        cs2str: CassandraPrimitivesToStringlist[CS],
        ev0: ¬¬[V] <:< (Set[X] ∨ List[X]),
	      ev1: CassandraPrimitive[X])= {
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
		    rowmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[RS, RK],
		    colmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[CS, CK],
  			a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
  			a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
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

  
  override protected def putValue[S <: BuiltStatement, U <: BuiltStatement, T](value: V, stmt: S, token: Option[T]): U = {
    def setValueFunction[W]: (String, W) => U = (colName, value) => stmt match {
      case update: Update.Assignments => update.and(QueryBuilder.set(colName, value)).asInstanceOf[U]
      case insert: Insert => insert.value(colName, value).asInstanceOf[U]
    }
    value match {
      case set: Set[X] => setValueFunction(valueColumnName, set.map(v => ev2.toCType(v)).toSet.asJava)
      case list: List[X] => setValueFunction(valueColumnName, list.map(v => ev2.toCType(v)).toList.asJava)
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
      }.flatMap(stmt => sync.merge.lock(lockId, {
   	      val origValue = Await.result(get((rk, ck)))
   	      // thread-safe: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html
   	      columnFamily.session.getSession.execute(stmt)
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
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(implicit equiv: Equiv[T], 
      cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, (RK, CK), V] 
      with IterableStore[(RK, CK), V] with Closable = new CQLCassandraCollectionStore[RK, CK, V, X, RS, CS](columnFamily, 
            rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency, 
            poolSize, batchType, ttl)(mergeSemigroup, sync) with CassandraCASStoreSimple[T, (RK, CK), V] {
    override protected def deleteColumns: String = s"$valueColumnName , $tokenColumnName"
    override def cas(token: Option[T], kv: ((RK, CK), V))(implicit ev1: Equiv[T]): Future[Boolean] =
      casImpl(token, kv, createPutQuery[(RK, CK), T](token, Some(putToken(tokenColumnName, 
          tokenFactory, cassTokenSerializer)))(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    override def get(key: (RK, CK))(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = 
      getImpl(key, createGetQuery(_), cassTokenSerializer, getRowValue(_), tokenColumnName, columnFamily, consistency)(ev1)
  }
}
