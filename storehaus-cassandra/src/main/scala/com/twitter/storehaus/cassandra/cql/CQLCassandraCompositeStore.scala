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

import com.twitter.util.{Future, Duration, FuturePool}
import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, ResultSet, Row}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{QueryBuilder, BuiltStatement, Insert, Delete, Select, Update, Clause}
import com.twitter.storehaus.Store
import com.twitter.storehaus.WithPutTtl
import java.util.concurrent.Executors
import com.websudos.phantom.CassandraPrimitive
import scala.collection.immutable.Nil
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import shapeless._
import HList._
import Traversables._
import Nat._
import UnaryTCConstraint._

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
	        createColumnListing(List(valueColumnName), List(valueSerializer.cassandraType)) +
	        "PRIMARY KEY ((\"" + rowkeyColumnNames.mkString("\", \"") + "\"), \"" +
	        colkeyColumnNames.mkString("\", \"") + "\"));"
    columnFamily.session.getSession.execute(stmt)
  }
}

class CQLCassandraCompositeStore[RK <: HList, CK <: HList, V, RS <: HList, CS <: HList] (
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
  (cassSerValue: CassandraPrimitive[V])(
    implicit evrow: MappedAux[RK, CassandraPrimitive, RS],
    evcol: MappedAux[CK, CassandraPrimitive, CS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result[RK, RS],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result[CK, CS],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK], 
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK],
    rsUTC: *->*[CassandraPrimitive]#λ[RS],
    csUTC: *->*[CassandraPrimitive]#λ[CS])
  extends AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS] (columnFamily, rowkeySerializer, rowkeyColumnNames,
    colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType, ttl)
  with WithPutTtl[(RK, CK), V, CQLCassandraCompositeStore[RK, CK, V, RS, CS]] {

  import AbstractCQLCassandraCompositeStore._

  override def withPutTtl(ttl: Duration): CQLCassandraCompositeStore[RK, CK, V, RS, CS] = new CQLCassandraCompositeStore[RK, CK, V, RS, CS](
    columnFamily, rowkeySerializer, rowkeyColumnNames, colkeySerializer, colkeyColumnNames, valueColumnName, consistency, poolSize, batchType,
    Option(ttl))(cassSerValue)

  override protected def putValue(value: V, update: Update): Update.Assignments = update.`with`(QueryBuilder.set(valueColumnName, value))

  override protected def getValue(result: ResultSet): Option[V] = cassSerValue.fromRow(result.one(), valueColumnName)

  override def getRowValue(row: Row): V = cassSerValue.fromRow(row, valueColumnName).get
}
