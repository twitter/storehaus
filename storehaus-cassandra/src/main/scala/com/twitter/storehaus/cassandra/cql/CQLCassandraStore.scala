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
import com.datastax.driver.core.policies.{Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, QueryBuilder}
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, ReadableStoreProxy, Store, WithPutTtl}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Await, Future, Duration, FuturePool, Promise, Try, Throw, Return}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors

object CQLCassandraStore {
  
  def createColumnFamily[K : CassandraPrimitive, V : CassandraPrimitive] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      ) = {
    createColumnFamilyWithToken[K, V, String](columnFamily, None, "", valueColumnName, keyColumnName)
  }

  def createColumnFamilyWithToken[K : CassandraPrimitive, V : CassandraPrimitive, T] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		tokenSerializer: Option[CassandraPrimitive[T]],
		tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
		valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      ) = {
    val keySerializer = implicitly[CassandraPrimitive[K]]
    val valueSerializer = implicitly[CassandraPrimitive[V]]
    columnFamily.session.createKeyspace
    val stmt = "CREATE TABLE IF NOT EXISTS " + columnFamily.getPreparedNamed +
	        " ( \"" + keyColumnName + "\" " + keySerializer.cassandraType + " PRIMARY KEY, \"" + 
	        valueColumnName + "\" " + valueSerializer.cassandraType + 
	        (tokenSerializer match {
	        	case Some(tokenSer) => ", \"" + tokenColumnName + "\" " + tokenSer.cassandraType
	        	case _ => ""
    		}) + ");"
    columnFamily.session.getSession.execute(stmt)
  }

}

/**
 * Simple key-value store
 */
class CQLCassandraStore[K : CassandraPrimitive, V : CassandraPrimitive] (
		override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		val valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		val keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
		val consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
		val ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
	extends AbstractCQLCassandraSimpleStore[K, V](poolSize, columnFamily, keyColumnName, consistency, batchType, ttl)
	with Store[K, V] 
    with WithPutTtl[K, V, CQLCassandraStore[K, V]] 
    with QueryableStore[String, (K, V)] 
    with IterableStore[K, V] 
    with CassandraCASStore[K, V] {
  val valueSerializer = implicitly[CassandraPrimitive[V]]
  
  override def withPutTtl(ttl: Duration): CQLCassandraStore[K, V] = new CQLCassandraStore(columnFamily, 
      valueColumnName, keyColumnName, consistency, poolSize, batchType, Some(ttl))
  
  protected def deleteColumns: Option[String] = Some(valueColumnName)
  
  protected def createPutQuery[K1 <: K](kv: (K1, V)) = {
    val (key, value) = kv
    QueryBuilder.insertInto(columnFamily.getPreparedNamed).value(keyColumnName, key).value(valueColumnName, value)
  }
  
  override def getKeyValueFromRow(row: Row): (K, V) = (keySerializer.fromRow(row, keyColumnName).get, valueSerializer.fromRow(row, valueColumnName).get)
  
  /**
   * return comma separated list of key and value column name
   */
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    AbstractCQLCassandraCompositeStore.quote(sb, keyColumnName, true)
    AbstractCQLCassandraCompositeStore.quote(sb, valueColumnName)
    sb.toString
  }
  
  override def getValue(result: ResultSet): Option[V] = valueSerializer.fromRow(result.one(), valueColumnName)
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(
      implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, K, V] with IterableStore[K, V] = 
        new CQLCassandraStore[K, V](columnFamily, valueColumnName, keyColumnName, consistency, poolSize, batchType, ttl) 
        with CassandraCASStoreSimple[T, K, V] with ReadableStore[K, V] {
    import QueryBuilder.set
    override protected def deleteColumns: Option[String] = Some(s"$valueColumnName , $tokenColumnName")
    override protected def createPutQuery[K1 <: K](kv: (K1, V)) = super.createPutQuery(kv)
    override def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[Boolean] = { 
      def putQueryConversion(kv: (K, Option[V])): BuiltStatement = token match {
        case None => createPutQuery[K](kv._1, kv._2.get).value(tokenColumnName, cassTokenSerializer.toCType(tokenFactory.createNewToken)).ifNotExists()
        case Some(token) => QueryBuilder.update(columnFamily.getPreparedNamed).`with`(set(valueColumnName, kv._2)).
          and(set(tokenColumnName, tokenFactory.createNewToken)).where(QueryBuilder.eq(keyColumnName, kv._1)).
          onlyIf(QueryBuilder.eq(tokenColumnName, cassTokenSerializer.toCType(token)))
      }
      casImpl(token, kv, putQueryConversion(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def get(key: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = {
	    def rowExtractor(r: Row): V = valueSerializer.fromRow(r, valueColumnName).get
      getImpl(key, createGetQuery(_), cassTokenSerializer, rowExtractor(_), tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = super[ReadableStore].multiGet(ks)
  }
}
