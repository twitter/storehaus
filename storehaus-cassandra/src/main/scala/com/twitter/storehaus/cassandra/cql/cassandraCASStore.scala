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

import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Select, Insert, Update}
import com.twitter.storehaus.IterableStore
import com.twitter.util.{Closable, Future}
import com.websudos.phantom.CassandraPrimitive
import java.net.InetAddress
import java.util.UUID
import scala.util.Random
import scala.util.hashing.MurmurHash3
import shapeless.HList

/**
 * factory to create token based Cassandra-stores
 */
trait CassandraCASStore[K, V] {
  def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)
    (implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): 
    CASStore[T, K, V] with IterableStore[K, V] with Closable
}

/**
 * how to derive a new Token for a given key
 */
trait TokenFactory[T] {
  def createNewToken: T
  def createIfAndComparison(columnName: String, t: T): String
}

object TokenFactory {
  implicit val longTokenFactory = new TokenFactory[Long] {
    // at about 2^32 inserts probability of clash is about 50% (if random is really random)
    override def createNewToken: Long = Random.nextLong 
    override def createIfAndComparison(columnName: String, t: Long): String = s""" IF "$columnName"=${t.toString} """
  }
  implicit val uuidTokenFactory = new TokenFactory[UUID] {
    // at about 2^64 inserts probability of clash is about 50% (if random is really random)
    override def createNewToken: UUID = UUID.randomUUID
    override def createIfAndComparison(columnName: String, t: UUID): String = s""" IF "$columnName"=${t.toString} """
  }
  implicit val hostBasedUUIDTokenFactory = new TokenFactory[UUID] {
    // this UUID = random(long) + hash(host-ip) + hash(pid)
    override def createNewToken: UUID = {
      val hostHash = MurmurHash3.stringHash(InetAddress.getLocalHost.getHostAddress)
      val threadHash = MurmurHash3.stringHash(Thread.currentThread().getName())
      new UUID(Random.nextLong, (hostHash.toLong << 32) | threadHash.toLong)
    }
    override def createIfAndComparison(columnName: String, t: UUID): String = s""" IF "$columnName"=${t.toString} """
  }
}

/**
 * Basic implementation of CASStore for Cassandra
 */
trait CassandraCASStoreSimple[T, K, V] extends CASStore[T, K, V] { self: AbstractCQLCassandraStore[K, V] =>
  def casImpl(token: Option[T], 
      kv: (K, V), 
      createQuery: ((K, Option[V])) => BuiltStatement, 
      tokenFactory: TokenFactory[T], 
      tokenColumnName: String,
      columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
      consistency: ConsistencyLevel)(implicit ev1: Equiv[T]): Future[Boolean] = futurePool {
    val simpleStatement = createQuery((kv._1, Some(kv._2))).setConsistencyLevel(consistency)
    val resultSet = columnFamily.session.getSession.execute(simpleStatement)
    resultSet.one().getBool(0)
  }
  def getImpl(key: K,
      createQuery: (K) => Select.Where,
      tokenSerializer: CassandraPrimitive[T],
      rowExtractor: (Row) => V,
      tokenColumnName: String,
      columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
      consistency: ConsistencyLevel)(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = futurePool {
    val result = columnFamily.session.getSession.execute(createQuery(key).limit(1).setConsistencyLevel(consistency))
    result.isExhausted() match {
      case false => {
        val row = result.one()
        Some((rowExtractor(row), tokenSerializer.fromRow(row, tokenColumnName).get)) 
      }
      case true => None
    }
  }
}

case class CassandraTokenInformation[T](cassTokenSerializer: CassandraPrimitive[T], token: T, tokenColumn: String)
