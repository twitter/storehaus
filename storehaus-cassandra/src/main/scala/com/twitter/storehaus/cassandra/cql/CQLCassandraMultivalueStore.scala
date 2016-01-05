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
import com.datastax.driver.core.querybuilder.{Assignment, BuiltStatement, Clause, Delete, Insert, QueryBuilder, Select, Update}
import com.twitter.util.{Closable, Duration, Future, FuturePool}
import com.twitter.storehaus.{IterableStore, Store, WithPutTtl}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors
import scala.collection.immutable.Nil
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import shapeless.HList
import shapeless.ops.hlist.{ Mapper, Mapped, ToList}
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable.traversableOps

/**
 *  Cassandra store for multiple values which can be matched to columns in Cassandra.
 *  Values must be provided as part of an HList to be type-safe
 *  
 *  @author Andreas Petter
 */
object CQLCassandraMultivalueStore {
  import AbstractCQLCassandraCompositeStore._
  
  def createColumnFamily[RS <: HList, CS <: HList, VS <: HList, MRKResult <: HList, MCKResult <: HList, MVResult <: HList] (
      columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializers: VS,
	valueColumnNames: List[String])
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
      cs2str: CassandraPrimitivesToStringlist[CS],
      vs2str: CassandraPrimitivesToStringlist[VS])= {
    createColumnFamilyWithToken[RS, CS, VS, MRKResult, MCKResult, MVResult, String](columnFamily, rowkeySerializers, rowkeyColumnNames,
        colkeySerializers, colkeyColumnNames, valueSerializers, valueColumnNames, None, "")
  }
  
  def createColumnFamilyWithToken[RS <: HList, CS <: HList, VS <: HList, MRKResult <: HList, MCKResult <: HList, MVResult <: HList, T] (
	columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	rowkeySerializers: RS,
	rowkeyColumnNames: List[String],
	colkeySerializers: CS,
	colkeyColumnNames: List[String],
	valueSerializers: VS,
	valueColumnNames: List[String],
	tokenSerializer: Option[CassandraPrimitive[T]] = None,
	tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)
	(implicit rs2str: CassandraPrimitivesToStringlist[RS],
      cs2str: CassandraPrimitivesToStringlist[CS],
      vs2str: CassandraPrimitivesToStringlist[VS])= {
    columnFamily.session.createKeyspace
    val rowKeyStrings = rowkeySerializers.stringlistify
    val colKeyStrings = colkeySerializers.stringlistify
    val valueStrings = valueSerializers.stringlistify
    val stmt = s"""CREATE TABLE IF NOT EXISTS ${columnFamily.getPreparedNamed} (""" +
    		createColumnListing(rowkeyColumnNames, rowKeyStrings) +
	        createColumnListing(colkeyColumnNames, colKeyStrings) +
	        createColumnListing(valueColumnNames, valueStrings) +
	        (tokenSerializer match {
	        	case Some(tokSer) => ", \"" + tokenColumnName + "\" " + tokSer.cassandraType + ", "
	        	case _ => ""
    		}) +
	        "PRIMARY KEY ((\"" + rowkeyColumnNames.mkString("\", \"") + "\"), \"" +
	        colkeyColumnNames.mkString("\", \"") + "\"));"
    columnFamily.session.getSession.execute(stmt)
  }
}

class CQLCassandraMultivalueStore[RK <: HList, CK <: HList, V <: HList, RS <: HList, CS <: HList, VS <: HList] (
  override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
  (cassSerValue: VS, valueColumnNameList: List[String])(
    implicit evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
    evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
    evval: Mapped.Aux[V, CassandraPrimitive, VS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[RS, RK],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[CS, CK],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
    vAsList: ToList[V, Any],
    vsAsList: ToList[VS, Any],
    vsFromList: FromTraversable[V])
  extends AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS] (columnFamily, rowkeySerializer, rowkeyColumnNames,
    colkeySerializer, colkeyColumnNames, "", consistency, poolSize, batchType, ttl)
  with WithPutTtl[(RK, CK), V, CQLCassandraMultivalueStore[RK, CK, V, RS, CS, VS]] 
  with CassandraCASStore[(RK, CK), V] {

  import AbstractCQLCassandraCompositeStore._

  override def withPutTtl(ttl: Duration): CQLCassandraMultivalueStore[RK, CK, V, RS, CS, VS] = new CQLCassandraMultivalueStore[RK, CK, V, RS, CS, VS](
    columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, consistency, poolSize, batchType,
    Option(ttl))(cassSerValue, valueColumnNameList)

  override protected def deleteColumns: String = valueColumnNameList.mkString(" , ")
  
  override protected def putValue[S <: BuiltStatement, U <: BuiltStatement, T](value: V, stmt: S, token: Option[T]): U = {
    val zipped = value.toList.zip(valueColumnNameList)
    stmt match { 
      case update: Update.Assignments => 
        val sets = zipped.map(colvalues => QueryBuilder.set(s""""${colvalues._2}"""", colvalues._1))
        sets.foldLeft(update)((acc, clause) => acc.and(clause)).asInstanceOf[U]
      case insert: Insert => zipped.foldLeft(insert)((acc, colvalues) => acc.value(s""""${colvalues._2}"""", colvalues._1)).asInstanceOf[U]
    }
  }

  override protected def getValue(result: ResultSet): Option[V] = Some(getRowValue(result.one()))

  override def getRowValue(row: Row): V = cassSerValue.toList.zip(valueColumnNameList).
    map(sercol => sercol._1.asInstanceOf[CassandraPrimitive[_]].fromRow(row, s""""${sercol._2}"""").get).toHList[V].get
  
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    rowkeyColumnNames.map(quote(sb, _, true))
    colkeyColumnNames.map(quote(sb, _, true))
    valueColumnNameList.map(name => quote(sb, name, name != valueColumnNameList.last))
    sb.toString
  }
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(implicit equiv: Equiv[T],
      cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, (RK, CK), V]
      with IterableStore[(RK, CK), V] with Closable = new CQLCassandraMultivalueStore[RK, CK, V, RS, CS, VS](
      columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, consistency, poolSize, 
      batchType, ttl)(cassSerValue, valueColumnNameList) with CassandraCASStoreSimple[T, (RK, CK), V] {
    override protected def deleteColumns: String = s"${super.deleteColumns} , $tokenColumnName"
    override def cas(token: Option[T], kv: ((RK, CK), V))(implicit ev1: Equiv[T]): Future[Boolean] =
      casImpl(token, kv, createPutQuery[(RK, CK), T](token, Some(putToken(tokenColumnName, 
          tokenFactory, cassTokenSerializer)))(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    override def get(key: (RK, CK))(implicit ev1: Equiv[T]): Future[Option[(V, T)]] =
      getImpl(key, createGetQuery(_), cassTokenSerializer, getRowValue(_), tokenColumnName, columnFamily, consistency)(ev1)
  }
}
