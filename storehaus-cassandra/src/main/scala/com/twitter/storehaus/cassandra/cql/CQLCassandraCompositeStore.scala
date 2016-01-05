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

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Clause, Insert, QueryBuilder, Update}
import com.twitter.util.{Closable, Duration, Future}
import com.twitter.storehaus.{IterableStore, Store, WithPutTtl}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import shapeless.HList
import shapeless.ops.hlist.{Mapped, Mapper, ToList}

/**
 *  Cassandra store for fixed composite keys, allows to do slice queries over column slices.
 *  Row- (RK) and Column-keys (CK) are provided as HLists in a Tuple2 to achieve type safety
 *  
 *  @author Andreas Petter
 */
object CQLCassandraCompositeStore {
  import AbstractCQLCassandraCompositeStore._
  
  /**
   * Optionally this method can be used to setup the column family on the Cassandra cluster.
   * Implicits are from shapeless. Types in the HList should conform to CassandraPrimitives from
   * the phantom Library. 
   */
  def createColumnFamily[RS <: HList, CS <: HList, V, MRKResult <: HList, MCKResult <: HList] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializer: CassandraPrimitive[V],
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  tablecomment: Option[String] = None)
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
      cs2str: CassandraPrimitivesToStringlist[CS])= {
    createColumnFamilyWithToken[RS, CS, V, MRKResult, MCKResult, String](columnFamily, rowkeySerializers, rowkeyColumnNames,
        colkeySerializers, colkeyColumnNames, valueSerializer, None, "", valueColumnName)
  }
  
  def createColumnFamilyWithToken[RS <: HList, CS <: HList, V, MRKResult <: HList, MCKResult <: HList, T] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializer: CassandraPrimitive[V],
	tokenSerializer: Option[CassandraPrimitive[T]] = None,
	tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
	valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  tablecomment: Option[String] = None)
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
	        createColumnListing(List(valueColumnName), List(valueSerializer.cassandraType)) +
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

class CQLCassandraCompositeStore[RK <: HList, CK <: HList, V, RS <: HList, CS <: HList] (
  override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
  (cassSerValue: CassandraPrimitive[V])(
    implicit evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
    evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[RS, RK],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[CS, CK],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS])
  extends AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS] (columnFamily, rowkeySerializer, rowkeyColumnNames,
    colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType, ttl)
  with WithPutTtl[(RK, CK), V, CQLCassandraCompositeStore[RK, CK, V, RS, CS]] 
  with CassandraCASStore[(RK, CK), V] {

  import AbstractCQLCassandraCompositeStore._

  override def withPutTtl(ttl: Duration): CQLCassandraCompositeStore[RK, CK, V, RS, CS] = new CQLCassandraCompositeStore[RK, CK, V, RS, CS](
    columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType,
    Option(ttl))(cassSerValue)

  override protected def putValue[S <: BuiltStatement, U <: BuiltStatement, T](value: V, stmt: S, token: Option[T]): U = stmt match {
    case update: Update.Assignments => update.and(QueryBuilder.set(valueColumnName, cassSerValue.toCType(value))).asInstanceOf[U]
    case insert: Insert => insert.value(valueColumnName, cassSerValue.toCType(value)).asInstanceOf[U]
  }

  override protected def getValue(result: ResultSet): Option[V] = cassSerValue.fromRow(result.one(), valueColumnName)

  override def getRowValue(row: Row): V = cassSerValue.fromRow(row, valueColumnName).get

  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(implicit equiv: Equiv[T],
      cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, (RK, CK), V] with IterableStore[(RK, CK), V]
      with Closable = new CQLCassandraCompositeStore[RK, CK, V, RS, CS](columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer,
          colkeyColumnNames, valueColumnName, consistency, poolSize, batchType, ttl)(cassSerValue) with 
          CassandraCASStoreSimple[T, (RK, CK), V] {
    override protected def deleteColumns: String = s"$valueColumnName , $tokenColumnName"
    override def cas(token: Option[T], kv: ((RK, CK), V))(implicit ev1: Equiv[T]): Future[Boolean] =
      casImpl(token, kv, createPutQuery[(RK, CK), T](token, Some(putToken(tokenColumnName, 
          tokenFactory, cassTokenSerializer)))(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    override def get(key: (RK, CK))(implicit ev1: Equiv[T]): Future[Option[(V, T)]] =
      getImpl(key, createGetQuery(_), cassTokenSerializer, getRowValue(_), tokenColumnName, columnFamily, consistency)(ev1)
  }
}
