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

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement}
import com.datastax.driver.core.policies.{Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Insert, QueryBuilder}
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, ReadableStoreProxy, Store, WithPutTtl}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Await, Future, Duration, FuturePool, Promise, Try, Throw, Return}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors
import shapeless.Tuples._
import shapeless._
import FromTraversable._
import Traversables._
import UnaryTCConstraint._


object CQLCassandraStoreTupleValues {
  import AbstractCQLCassandraCompositeStore._
  
  def createColumnFamily[K : CassandraPrimitive, VS <: HList, MVResult <: HList] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnNames: List[String],
		valueSerializers: VS,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      )(implicit vss: MapperAux[keyStringMapping.type, VS, MVResult],
       tov: ToList[MVResult, String]) = {
    createColumnFamilyWithToken[K, VS, MVResult, String](columnFamily, valueColumnNames, valueSerializers, None, "", keyColumnName)
  }

  def createColumnFamilyWithToken[K : CassandraPrimitive, VS <: HList, MVResult <: HList, T] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnNames: List[String],
		valueSerializers: VS,
		tokenSerializer: Option[CassandraPrimitive[T]],
		tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      )(implicit vss: MapperAux[keyStringMapping.type, VS, MVResult],
       tov: ToList[MVResult, String]) = {
    val valSerStrings = valueSerializers.map(keyStringMapping).toList	
    val keySerializer = implicitly[CassandraPrimitive[K]]
    columnFamily.session.createKeyspace
    val stmt = "CREATE TABLE IF NOT EXISTS " + columnFamily.getPreparedNamed +
	        " ( \"" + keyColumnName + "\" " + keySerializer.cassandraType + " PRIMARY KEY, " + 
	        AbstractCQLCassandraCompositeStore.createColumnListing(valueColumnNames, valSerStrings) + 
	        (tokenSerializer match {
	        	case Some(tokenSer) => ", \"" + tokenColumnName + "\" " + tokenSer.cassandraType
	        	case _ => ""
    		}) + ");"
    columnFamily.session.getSession.execute(stmt)
  }
}

/**
 * Simple key-value store, taking tuples as values. Entries must be of type Option (None represents that the
 * values should/have not be/been set, while Some represents a set value). This is needed as in Cassandra 
 * Columns may not be present in the column family. HLists must be provided to allow abstraction over arity 
 * of tuples.
 */
class CQLCassandraStoreTupleValues[K: CassandraPrimitive, V <: Product, VL <: HList, VS <: HList] (
		val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		val valueColumnNames: List[String],
		val valueSerializers: VS,
		val keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
		val consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
		val ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
		   implicit ev2: HListerAux[V, VL],
		   ev3: TuplerAux[VL, V],
		   vAsList: ToList[VL, Any], 
		   vsAsList: ToList[VS, CassandraPrimitive[_]],
		   vsFromList: FromTraversable[VL])
		   // this implicit may crash the Scala compiler: rsUTC: *->*[CassandraPrimitive]#λ[VS])
	extends AbstractCQLCassandraSimpleStore[K, V](poolSize, columnFamily, keyColumnName, consistency, batchType, ttl)
	with Store[K, V] 
    with WithPutTtl[K, V, CQLCassandraStoreTupleValues[K, V, VL, VS]] 
    with CassandraCascadingRowMatcher[K, V]
    with QueryableStore[String, (K, V)] 
    with IterableStore[K, V] 
    with CassandraCASStore[K, V] {
  
  override def withPutTtl(ttl: Duration): CQLCassandraStoreTupleValues[K, V, VL, VS] = new CQLCassandraStoreTupleValues[K, V, VL, VS](columnFamily, 
      valueColumnNames, valueSerializers, keyColumnName, consistency, poolSize, batchType, Some(ttl))

  protected def deleteColumns: String = valueColumnNames.mkString(" , ")
  
  /**
   * puts will not delete single columns if they are set to None. But it will delete all columns if the
   * hole Option[TupleX] is None. 
   */
  protected def createPutQuery[K1 <: K](kv: (K1, V)): Insert = {
    val sets = kv._2.hlisted.toList.zip(valueColumnNames)
    sets.foldLeft(QueryBuilder.insertInto(columnFamily.getPreparedNamed))((insert, values) => values._1 match {
      case Some(v) => insert.value(values._2, v)
      case None => insert
    })
  }
  
  override def getKeyValueFromRow(row: Row): (K, V) = (keySerializer.fromRow(row, keyColumnName).get, getRowValue(row).tupled)
  
  /**
   * get will return a tupleX with entries of type Option[T_x], each being None on unavailable or Some on stored 
   */
  override protected def getValue(result: ResultSet): Option[V] = Some(getRowValue(result.one()).tupled)
  
  protected def getRowValue(row: Row): VL = {
    valueSerializers.toList.zip(valueColumnNames).map(sercol => sercol._1.fromRow(row, sercol._2)).toHList[VL].get
  }
      
  /**
   * return comma separated list of key and value column name
   */
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    AbstractCQLCassandraCompositeStore.quote(sb, keyColumnName, true)
    valueColumnNames.map(name => AbstractCQLCassandraCompositeStore.quote(sb, name, name != valueColumnNames.last))
    sb.toString
  }
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(
      implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, K, V] with IterableStore[K, V] = new CQLCassandraStoreTupleValues[K, V, VL, VS](
      columnFamily, valueColumnNames, valueSerializers, keyColumnName, consistency, poolSize, batchType, ttl) with CassandraCASStoreSimple[T, K, V] {
    override protected def deleteColumns: String = s"${super.deleteColumns} , $tokenColumnName"
    override protected def createPutQuery[K1 <: K](kv: (K1, V)) = super.createPutQuery(kv).value(tokenColumnName, tokenFactory.createNewToken)    
    override def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[Boolean] = { 
      def putQueryConversion(kv: (K, Option[V])): BuiltStatement = createPutQuery[K](kv._1, kv._2.get)  
      casImpl(token, kv, putQueryConversion(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def get(key: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = {
	  def rowExtractor(r: Row): V = super.getRowValue(r).tupled
	  getImpl(key, createGetQuery(_), cassTokenSerializer, rowExtractor(_), tokenColumnName, columnFamily, consistency)(ev1)
    }
  }
}
