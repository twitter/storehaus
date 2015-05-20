/*
 * Copyright 2013 Twitter Inc.
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

package com.twitter.storehaus.memcache

import com.twitter.algebird.Semigroup
import com.twitter.bijection.{ Bijection, Codec, Injection }
import com.twitter.bijection.netty.Implicits._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcachedx.KetamaClientBuilder
import com.twitter.finagle.memcachedx.protocol.text.Memcached
import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.util.{ Duration, Future, Time }
import com.twitter.finagle.memcachedx.{ GetResult, Client }
import com.twitter.storehaus.{ FutureOps, Store, WithPutTtl }
import com.twitter.storehaus.algebra.MergeableStore
import org.jboss.netty.buffer.ChannelBuffer

import Store.enrich

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object MemcacheStore {
  import HashEncoder.keyEncoder

  // Default Memcached TTL is one day.
  // For more details of setting expiration time for items in Memcached, please refer to
  // https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L79
  val DEFAULT_TTL = 1.day

  // This flag used on "set" operations. Search this page for "flag"
  // for more info on the following variable:
  // http://docs.libmemcached.org/memcached_set.html
  val DEFAULT_FLAG = 0

  val DEFAULT_CONNECTION_LIMIT = 1
  val DEFAULT_TIMEOUT = 1.seconds
  val DEFAULT_RETRIES = 2

  def apply(client: Client, ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG) =
    new MemcacheStore(client, ttl, flag)

  def defaultClient(
    name: String,
    nodeString: String,
    retries: Int = DEFAULT_RETRIES,
    timeout: Duration = DEFAULT_TIMEOUT,
    hostConnectionLimit: Int = DEFAULT_CONNECTION_LIMIT): Client = {
    val builder = ClientBuilder()
      .name(name)
      .retries(retries)
      .tcpConnectTimeout(timeout)
      .requestTimeout(timeout)
      .connectTimeout(timeout)
      .readerIdleTimeout(timeout)
      .hostConnectionLimit(hostConnectionLimit)
      .codec(Memcached())

    KetamaClientBuilder()
      .clientBuilder(builder)
      .nodes(nodeString)
      .build()
  }

  /**
    * Returns a Memcache-backed Store[K, V] that uses
    * implicitly-supplied Injection instances from K and V ->
    * Array[Byte] to manage type conversion.
    */
  def typed[K: Codec, V: Codec](client: Client, keyPrefix: String,
    ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG): Store[K, V] = {
    implicit val valueToBuf = Injection.connect[V, Array[Byte], ChannelBuffer]
    MemcacheStore(client, ttl, flag).convert(keyEncoder[K](keyPrefix))
  }

  /**
    * Returns a Memcache-backed MergeableStore[K, V] that uses
    * implicitly-supplied Injection instances from K and V ->
    * Array[Byte] to manage type conversion. The Semigroup[V] is also
    * pulled in implicitly.
    */
  def mergeable[K: Codec, V: Codec: Semigroup](client: Client, keyPrefix: String,
    ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG): MergeableStore[K, V] =
    MergeableStore.fromStore(
      MemcacheStore.typed(client, keyPrefix, ttl, flag)
    )

  /**
   * Returns a Memcache-backed MergeableStore[K, V] that uses
   * compare-and-swap with retries. It supports multiple concurrent
   * writes to the same key and is useful when one thread/node does not
   * own a key space.
   */
  def mergeableWithCAS[K, V: Semigroup](client: Client, retries: Int,
    ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG)(kfn: K => String)
      (implicit inj: Injection[V, ChannelBuffer]): MergeableStore[K, V] =
    MergeableMemcacheStore[K, V](client, ttl, flag, retries)(kfn)(inj, implicitly[Semigroup[V]])
}

class MemcacheStore(val client: Client, val ttl: Duration, val flag: Int)
  extends Store[String, ChannelBuffer]
  with WithPutTtl[String, ChannelBuffer, MemcacheStore]
{
  override def withPutTtl(ttl: Duration) = new MemcacheStore(client, ttl, flag)

  override def get(k: String): Future[Option[ChannelBuffer]] =
    client.get(k).flatMap {
      case None => Future.None
      case Some(buf) => Future.value(Some(ChannelBufferBuf.Owned.extract(buf)))
    }

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    val memcacheResult: Future[Map[String, Future[Option[ChannelBuffer]]]] =
      client.getResult(ks).map { result =>
        result.hits.mapValues { v =>
          Future.value(Some(BufChannelBuffer(v.value)))
        } ++ result.failures.mapValues(Future.exception)
      }
    FutureOps.liftValues(ks, memcacheResult, { (k: K1) => Future.value(Future.None) })
      .mapValues { _.flatten }
  }

  protected def set(k: String, v: ChannelBuffer) =
    client.set(k, flag, ttl.fromNow, ChannelBufferBuf.Owned(v))

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] =
    kv match {
      case (key, Some(value)) =>
        client.set(key, flag, ttl.fromNow, ChannelBufferBuf.Owned(value))
      case (key, None) => client.delete(key).unit
    }

  override def close(t: Time) = Future(client.release)
}
