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

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement}
import com.datastax.driver.core.policies.{Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.QueryBuilder
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
    createColumnFamily[K, V, String](columnFamily, None, valueColumnName, keyColumnName)
  }

  def createColumnFamily[K : CassandraPrimitive, V : CassandraPrimitive, T] (
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
		val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		val valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		val keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
		val consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
		val ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
	extends AbstractCQLCassandraStore[K, V](poolSize, columnFamily)
	with Store[K, V] 
    with WithPutTtl[K, V, CQLCassandraStore[K, V]] 
    with CassandraCascadingRowMatcher[K, V]
    with QueryableStore[String, (K, V)] 
    with IterableStore[K, V] 
    with CassandraCASStore[K, V] {
  val keySerializer = implicitly[CassandraPrimitive[K]]
  val valueSerializer = implicitly[CassandraPrimitive[V]]
  
  override def withPutTtl(ttl: Duration): CQLCassandraStore[K, V] = new CQLCassandraStore(columnFamily, 
      valueColumnName, keyColumnName, consistency, poolSize, batchType, Some(ttl))
  
  protected def deleteColumns: String = valueColumnName
  
  protected def createPutQuery[K1 <: K](kv: (K1, V)) = {
    val (key, value) = kv
    QueryBuilder.insertInto(columnFamily.getPreparedNamed).value(keyColumnName, key).value(valueColumnName, value)
  }
  
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool[Unit] {
    	val mutator = new BatchStatement(batchType)
    	kvs.foreach {
    	  case (key, Some(value)) => { 
    	    val builder = createPutQuery((key, value))
    	    ttl match {
    	      case Some(duration) => builder.using(QueryBuilder.ttl(duration.inSeconds))
    	      case _ =>
    	    }
    	    mutator.add(builder)
    	  }
    	  case (key, None) => mutator.add(
    	      QueryBuilder
    	        .delete(deleteColumns)
    	        .from(columnFamily.getName)
    	        .where(QueryBuilder.eq(keyColumnName, key)))
    	}
    	mutator.setConsistencyLevel(consistency)
    	columnFamily.session.getSession.execute(mutator)
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  protected def createGetQuery(key: K) = {
    QueryBuilder
        .select()
        .from(columnFamily.getPreparedNamed)
        .where(QueryBuilder.eq(keyColumnName, key))
  }
  
  override def get(key: K): Future[Option[V]] = {
    futurePool {
      val stmt = createGetQuery(key)
        .limit(1)
        .setConsistencyLevel(consistency)
      val result = columnFamily.session.getSession.execute(stmt)
      result.isExhausted() match {
        case false => {
          valueSerializer.fromRow(result.one(), valueColumnName)
        }
        case true => None
      }
    }
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
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(
      implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, K, V] with IterableStore[K, V] = new CQLCassandraStore[K, V](
      columnFamily, valueColumnName, keyColumnName, consistency, poolSize, batchType, ttl) with CASStore[T, K, V] {
    override protected def deleteColumns: String = s"$valueColumnName , $tokenColumnName"
    override protected def createPutQuery[K1 <: K](kv: (K1, V)) = super.createPutQuery(kv).value(tokenColumnName, tokenFactory.createNewToken)
    override def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[(V, T)] = futurePool {
      val statement = createPutQuery[K]((kv._1, kv._2)).setForceNoValues(false).getQueryString()
      val casCondition = token match {
        case Some(tok) => tokenFactory.createIfAndComparison(tokenColumnName, tok)
        case None => " IF NOT EXISTS"
      }
      val simpleStatement = new SimpleStatement(s"$statement $casCondition").setConsistencyLevel(consistency)
      val resultSet = columnFamily.session.getSession.execute(simpleStatement)
      println(resultSet.one())
      (kv._2, token.get)
    }
    override def get(key: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = futurePool {
      val result = columnFamily.session.getSession.execute(createGetQuery(key).limit(1).setConsistencyLevel(consistency))
      result.isExhausted() match {
        case false => {
          val row = result.one()
          Some((valueSerializer.fromRow(result.one(), valueColumnName).get, cassTokenSerializer.fromRow(row, tokenColumnName).get)) 
        }
        case true => None
      }
    }
  }
}
