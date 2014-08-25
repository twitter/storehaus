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

import com.twitter.util.{Await, Future, Duration, FuturePool}
import com.datastax.driver.core.{Statement, ConsistencyLevel, BatchStatement, Row}
import com.datastax.driver.core.policies.{ Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.twitter.storehaus.Store
import com.twitter.storehaus.WithPutTtl
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import java.util.concurrent.Executors
import com.websudos.phantom.CassandraPrimitive

object CQLCassandraStore {
  
  def createColumnFamily[K : CassandraPrimitive, V : CassandraPrimitive] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      ) = {
    val keySerializer = implicitly[CassandraPrimitive[K]]
    val valueSerializer = implicitly[CassandraPrimitive[V]]
    columnFamily.session.createKeyspace
    val stmt = "CREATE TABLE IF NOT EXISTS " + columnFamily.getPreparedNamed +
	        " ( \"" + keyColumnName + "\" " + keySerializer.cassandraType + " PRIMARY KEY, \"" + 
	        valueColumnName + "\" " + valueSerializer.cassandraType + ");"
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
	extends Store[K, V] 
    with WithPutTtl[K, V, CQLCassandraStore[K, V]] 
    with CassandraCascadingRowMatcher[K, V] {
  val keySerializer = implicitly[CassandraPrimitive[K]]
  val valueSerializer = implicitly[CassandraPrimitive[V]]
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  override def withPutTtl(ttl: Duration): CQLCassandraStore[K, V] = new CQLCassandraStore(columnFamily, 
      valueColumnName, keyColumnName, consistency, poolSize, batchType, Some(ttl))
  
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool[Unit] {
    	val mutator = new BatchStatement(batchType)
    	kvs.foreach {
    	  case (key, Some(value)) => { 
    	    val builder = QueryBuilder.insertInto(columnFamily.getPreparedNamed).value(keyColumnName, key).value(valueColumnName, value)
    	    ttl match {
    	      case Some(duration) => builder.using(QueryBuilder.ttl(duration.inSeconds))
    	      case _ =>
    	    }
    	    mutator.add(builder)
    	  }
    	  case (key, None) => mutator.add(
    	      QueryBuilder
    	        .delete(valueColumnName)
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

  override def get(key: K): Future[Option[V]] = {
    futurePool {
      val stmt = QueryBuilder
        .select()
        .from(columnFamily.getPreparedNamed)
        .where(QueryBuilder.eq(keyColumnName, key))
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
  
}
