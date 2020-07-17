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
import com.twitter.bijection.{Bijection, Codec, Injection}
import com.twitter.bijection.netty.Implicits._
import com.twitter.util.{Duration, Future, Time}
import com.twitter.finagle.memcached.Client
import com.twitter.storehaus.{FutureOps, Store, WithPutTtl}
import com.twitter.storehaus.algebra.MergeableStore
import Store.enrich
import com.twitter.finagle.client.DefaultPool
import com.twitter.finagle.Memcached
import com.twitter.finagle.factory.TimeoutFactory
import com.twitter.finagle.service.{Retries, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.transport.Transport
import java.util.concurrent.TimeUnit

import com.twitter.io.Buf
import com.twitter.io.Buf.ByteBuffer

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object MemcacheStore {
  import HashEncoder.keyEncoder

  // Default Memcached TTL is one day.
  // For more details of setting expiration time for items in Memcached, please refer to
  // https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L79
  val DEFAULT_TTL = Duration(1, TimeUnit.DAYS)

  // This flag used on "set" operations. Search this page for "flag"
  // for more info on the following variable:
  // http://docs.libmemcached.org/memcached_set.html
  val DEFAULT_FLAG = 0

  val DEFAULT_CONNECTION_LIMIT = 1
  val DEFAULT_TIMEOUT = Duration(1, TimeUnit.SECONDS)
  val DEFAULT_RETRIES = 2

  def apply(
      client: Client, ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG): MemcacheStore =
    new MemcacheStore(client, ttl, flag)

  def defaultClient(
    name: String,
    nodeString: String,
    retries: Int = DEFAULT_RETRIES,
    timeout: Duration = DEFAULT_TIMEOUT,
    hostConnectionLimit: Int = DEFAULT_CONNECTION_LIMIT): Client = {

    Memcached.client
      .withLabel(name)
      .withRequestTimeout(timeout)
      .withTransport
      .connectTimeout(timeout)
      .configured(Retries.Policy(RetryPolicy.tries(retries)))
      .configured(TimeoutFilter.Param(timeout))
      .configured(TimeoutFactory.Param(timeout))
      .configured(Transport.Liveness.param.default.copy(readTimeout = timeout))
      .configured(DefaultPool.Param.param.default.copy(high = hostConnectionLimit))
      .newTwemcacheClient(nodeString)
  }

  /**
    * Returns a Memcache-backed Store[K, V] that uses
    * implicitly-supplied Injection instances from K and V ->
    * Array[Byte] to manage type conversion.
    */
  def typed[K: Codec, V: Codec](client: Client, keyPrefix: String,
    ttl: Duration = DEFAULT_TTL, flag: Int = DEFAULT_FLAG): Store[K, V] = {
    implicit val arrayToBuf = Bijection.build[Array[Byte], Buf] {
      a => Buf.ByteArray.Owned(a)
    } {
      b => Buf.ByteArray.Owned.extract(b)
    }
    implicit val valueToBuf = Injection.connect[V, Array[Byte], Buf]
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
      (implicit inj: Injection[V, Buf]): MergeableStore[K, V] =
    MergeableMemcacheStore[K, V](client, ttl, flag, retries)(kfn)(inj, implicitly[Semigroup[V]])
}

class MemcacheStore(val client: Client, val ttl: Duration, val flag: Int)
  extends Store[String, Buf]
  with WithPutTtl[String, Buf, MemcacheStore] {

  override def withPutTtl(ttl: Duration): MemcacheStore = new MemcacheStore(client, ttl, flag)

  override def get(k: String): Future[Option[Buf]] = client.get(k)

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[Buf]]] = {
    val memcacheResult: Future[Map[String, Future[Option[Buf]]]] =
      client.getResult(ks).map { result =>
        result.hits.mapValues { v =>
          Future.value(Some(v.value))
        } ++ result.failures.mapValues(Future.exception)
      }
    FutureOps.liftValues(ks, memcacheResult, { (k: K1) => Future.value(Future.None) })
      .mapValues { _.flatten }
  }

  protected def set(k: String, v: Buf) =
    client.set(k, flag, ttl.fromNow, v)

  override def put(kv: (String, Option[Buf])): Future[Unit] =
    kv match {
      case (key, Some(value)) =>
        client.set(key, flag, ttl.fromNow, value)
      case (key, None) => client.delete(key).unit
    }

  override def close(t: Time): Future[Unit] = Future(client.close())
}
