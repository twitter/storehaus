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

import com.twitter.conversions.time._
import com.twitter.util.{ Future, Time }
import com.twitter.finagle.memcached.{ GetResult, Client }
import com.twitter.storehaus.Store
import org.jboss.netty.buffer.ChannelBuffer

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object MemcacheStore {
  // Default Memcached TTL is one day.
  val DEFAULT_TTL = Time.fromSeconds(24 * 60 * 60)

  // This flag used on "set" operations. Search this page for "flag"
  // for more info on the following variable:
  // http://docs.libmemcached.org/memcached_set.html
  val DEFAULT_FLAG = 0

  def apply(client: Client, ttl: Time = DEFAULT_TTL, flag: Int = DEFAULT_FLAG) =
    new MemcacheStore(client, ttl, flag)
}

class MemcacheStore(client: Client, ttl: Time, flag: Int) extends Store[String, ChannelBuffer] {

  override def get(k: String): Future[Option[ChannelBuffer]] = client.get(k)

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    val memcacheResult: Future[Map[String, Future[Option[ChannelBuffer]]]] =
      client.getResult(ks).map { result =>
        Store.zipWith(result.misses) { k => Future.None } ++
        result.hits.mapValues { v => Future.value(Some(v.value)) } ++
        result.failures.mapValues { Future.exception(_) }
      }
    ks.map { k => k -> memcacheResult.flatMap { _.apply(k) } }.toMap
  }

  protected def set(k: String, v: ChannelBuffer) = client.set(k, flag, ttl, v)

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] =
    kv match {
      case (key, Some(value)) => client.set(key, flag, ttl, value)
      case (key, None) => client.delete(key).unit
    }

  override def close { client.release }
}
