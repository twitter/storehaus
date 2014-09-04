package com.twitter.storehaus.cassandra.cql

import com.twitter.storehaus.IterableStore
import com.websudos.phantom.CassandraPrimitive
import java.util.UUID
import scala.util.Random
import scala.util.hashing.MurmurHash3
import java.net.InetAddress

/**
 * factory to create token based Cassandra-stores
 */
trait CassandraCASStore[K, V] {
  def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)
    (implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): 
    CASStore[T, K, V] with IterableStore[K, V]  
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
      new UUID(Random.nextLong, (hostHash << 32) | threadHash)
    }
    override def createIfAndComparison(columnName: String, t: UUID): String = s""" IF "$columnName"=${t.toString} """
  }
}