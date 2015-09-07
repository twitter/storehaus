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

import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select}
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement}
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, Store}
import com.twitter.util.{Duration, Future, FuturePool, Promise, Throw, Return}
import java.util.concurrent.{Executors, TimeUnit}
import org.slf4j.{ Logger, LoggerFactory }
import com.websudos.phantom.CassandraPrimitive
import com.twitter.util.Time
import com.datastax.driver.core.VersionNumber
import com.twitter.util.Try

abstract class AbstractCQLCassandraSimpleStore[K : CassandraPrimitive, V] (
    override val poolSize: Int, 
    override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
    keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
    consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
    batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
    ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION) 
  extends AbstractCQLCassandraStore[K, V](poolSize, columnFamily)
  with Store[K, V] {
  
  @transient private val log = LoggerFactory.getLogger(classOf[AbstractCQLCassandraSimpleStore[K, V]])

  log.debug(s"""Creating new AbstractCQLCassandraSimpleStore on ${columnFamily.session.cluster.hosts} with consistency=${consistency.name} and 
    load-balancing=${columnFamily.session.getCluster.getConfiguration.getPolicies.getLoadBalancingPolicy}""")

  val keySerializer = implicitly[CassandraPrimitive[K]]
  
  protected lazy val cassandraVersion: Option[VersionNumber] =
    // assumes that all hosts are the same version and there is at least one host available
    Try(columnFamily.session.getCluster.getMetadata.getAllHosts.iterator.next.getCassandraVersion).toOption

  
  protected def deleteColumns: Option[String]
  
  protected def createPutQuery[K1 <: K](kv: (K1, V)): Insert

  override def getKeyValueFromRow(row: Row): (K, V)
  
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
    	  case (key, None) => mutator.add(deleteColumns match {
    	    case None => QueryBuilder.delete().from(columnFamily.getName).where(QueryBuilder.eq(keyColumnName, key))
    	    case Some(cols) => QueryBuilder.delete(cols).from(columnFamily.getName).where(QueryBuilder.eq(keyColumnName, key))
    	  })
    	}
    	mutator.setConsistencyLevel(consistency)
    	columnFamily.session.getSession.execute(mutator)
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  /**
   * In case we have a Cassandra 2.1.x and a single row key, use implementation of multi-get.
   * Temporarily, data is stored in-memory for each multi-get.
   */
  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    import scala.math.Ordering.Implicits._
    import scala.collection.convert.WrapAsScala._
    cassandraVersion match {
      case Some(version) if (version.getMajor, version.getMinor) >= ((2, 1)) =>
        val clause = QueryBuilder.in(keyColumnName, ks.map(k => keySerializer.toCType(k)))
        val stmt = QueryBuilder.select().from(columnFamily.getPreparedNamed).where(clause).setConsistencyLevel(consistency)
        val future = futurePool { 
          val result = columnFamily.session.getSession.execute(stmt)
          result.all().map(getKeyValueFromRow(_))
        }
        ks.map(k => (k, future.map(rows => rows.find(_._1 == k).map(_._2)))).toMap
      case _ => super.multiGet[K1](ks)
    }
  }
    
  protected def createGetQuery(key: K): Select.Where = {
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
        case false => getValue(result)
        case true => None
      }
    }
  }
  
  override def close(deadline: Time) = super[AbstractCQLCassandraStore].close(deadline)
}
