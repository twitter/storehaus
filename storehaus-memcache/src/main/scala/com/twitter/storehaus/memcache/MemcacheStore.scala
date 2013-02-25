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

import com.twitter.bijection.{ Base64String, Bijection }
import com.twitter.conversions.time._
import com.twitter.util.{ Duration, Encoder, Future, FuturePool, Time }
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.{ Client, KetamaClientBuilder }
import com.twitter.storehaus.Store

import Bijection.connect

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// "weight" is a finagle implementation detail for the Memcached
// client. A higher weight gives a particular host precedence over
// others of lower weight in a client instance's host list.

case class HostConfig(host: String, port: Int = 11211, weight: Int = 1) {
  // TODO: Turn into a bijection.
  def toTuple = (host, port, weight)
}

object MemcacheStore {
  // Default Memcached TTL is one day.
  val DEFAULT_TTL = Time.fromSeconds(24 * 60 * 60)

  // implicitly convert the standard key serialization bijection into a
  // key->hashed string bijection, suitable for use with Memcached.
  implicit def toEncoder[Key](implicit bijection: Bijection[Key,Array[Byte]])
  : Encoder[Key,String] =
    new Encoder[Key,String] {
      val enc = (bijection andThen HashEncoder() andThen connect[Array[Byte], Base64String])
        override def encode(k: Key) = enc(k).str
    }

  // Instantiate a Memcached store using a Ketama client with the
  // given # of retries, request timeout and ttl.
  def apply[Key,Value](hosts: Seq[HostConfig],
                       retries: Int = 2,
                       timeout: Duration = 1.seconds,
                       ttl: Time = DEFAULT_TTL,
                       hostConnectionLimit: Int = 1)
  (implicit kb: Bijection[Key,Array[Byte]], vb: Bijection[Value,Array[Byte]]) =
    new MemcacheStore[Key,Value](hosts, kb, vb, retries, timeout, ttl, hostConnectionLimit)
}

class MemcacheStore[Key,Value](hosts: Seq[HostConfig],
                               kBijection: Bijection[Key, Array[Byte]],
                               vBijection: Bijection[Value, Array[Byte]],
                               retries: Int,
                               timeout: Duration,
                               ttl: Time,
                               hostConnectionLimit: Int)
extends Store[Key,Value] {

  lazy val enc =
    kBijection
      .andThen(HashEncoder())
      .andThen(connect[Array[Byte], Base64String])
      .andThen(Base64String.unwrap)

  // Memcache flag used on "set" operations. Search this page for
  // "flag" for more info on the following variable:
  // http://docs.libmemcached.org/memcached_set.html

  val MEMCACHE_FLAG = 0

  def clientBuilder(name: String, retries: Int, timeout: Duration) = {
    ClientBuilder()
      .name(name)
      .retries(retries)
      .tcpConnectTimeout(timeout)
      .requestTimeout(timeout)
      .connectTimeout(timeout)
      .readerIdleTimeout(timeout)
  }

  lazy val client = FuturePool.unboundedPool {
    val builder = clientBuilder("memcache_client", retries, timeout)
      .hostConnectionLimit(hostConnectionLimit)
      .codec(Memcached())
    KetamaClientBuilder()
      .clientBuilder(builder)
      .nodes(hosts.map(_.toTuple))
      .build()
      .withBytes // Adaptor to allow bytes vs channel buffers.
  }

  override def get(k: Key): Future[Option[Value]] =
    client flatMap { c =>
      c.get(enc(k)) map { opt =>
        opt map { vBijection.invert(_) }
      }
    }

  override def multiGet[K1<:Key](ks: Set[K1]): Map[K1, Future[Option[Value]]] = {
    val encMap = ks.map { k => (enc(k), k) }.toMap

    val retMapFuture = client flatMap { c => c.get(encMap.keys) }
    encMap map { case(encK, k) =>
      k -> retMapFuture.map { _.get(encK) map { bytes => vBijection.invert(bytes) } }
    }
  }

  protected def set(k: Key, v: Value) = {
    val valBytes = vBijection(v)
    client flatMap { _.set(enc(k), MEMCACHE_FLAG, ttl, valBytes) }
  }

  override def put(kv: (Key,Option[Value])): Future[Unit] =
    kv match {
      case (key, Some(value)) => client.flatMap { _.set(enc(key), MEMCACHE_FLAG, ttl, vBijection(value)) }
      case (key, None) => client.flatMap { _.delete(enc(key)) }.unit
    }

  override def close { client foreach { _.release } }
}
