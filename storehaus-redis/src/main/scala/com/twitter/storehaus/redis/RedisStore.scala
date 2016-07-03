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

package com.twitter.storehaus.redis

import com.twitter.util.{ Duration, Future, Time }
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.{ FutureOps, MissingValueException, Store, WithPutTtl }
import org.jboss.netty.buffer.ChannelBuffer

/**
 *  @author Doug Tangren
 */

object RedisStore {
  // For more details of setting expiration time for items in Redis, please refer to
  // http://redis.io/commands/expire
  object Default {
    val TTL: Option[Duration] = None
  }

  def apply(client: Client, ttl: Option[Duration] = Default.TTL): RedisStore =
    new RedisStore(client, ttl)
}

class RedisStore(val client: Client, ttl: Option[Duration])
  extends Store[ChannelBuffer, ChannelBuffer]
  with WithPutTtl[ChannelBuffer, ChannelBuffer, RedisStore] {

  override def withPutTtl(ttl: Duration): RedisStore = new RedisStore(client, Some(ttl))

  override def get(k: ChannelBuffer): Future[Option[ChannelBuffer]] =
    client.get(k)

  override def multiGet[K1 <: ChannelBuffer](
      ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    val redisResult: Future[Map[ChannelBuffer, Future[Option[ChannelBuffer]]]] = {
      // results are expected in the same order as keys
      // keys w/o mapped results are considered exceptional
      val keys = ks.toIndexedSeq.view
      client.mGet2(keys).map { result =>
        val zipped = keys.zip(result).map {
          case (k, v) => k -> Future.value(v)
        }.toMap
        zipped ++ keys.filterNot(zipped.isDefinedAt).map { k =>
          k -> Future.exception(new MissingValueException(k))
        }
      }
    }
    FutureOps.liftValues(ks, redisResult, { (k: K1) => Future.value(Future.None) })
      .mapValues { _.flatten }
  }

  protected def set(k: ChannelBuffer, v: ChannelBuffer) =
    ttl.map(exp => client.setEx(k, exp.inSeconds, v))
       .getOrElse(client.set(k, v))

  override def put(kv: (ChannelBuffer, Option[ChannelBuffer])): Future[Unit] =
    kv match {
      case (key, Some(value)) => set(key, value)
      case (key, None) => client.del(Seq(key)).unit
    }

  override def close(t: Time): Future[Unit] = client.quit.foreach { _ => client.close() }
}
