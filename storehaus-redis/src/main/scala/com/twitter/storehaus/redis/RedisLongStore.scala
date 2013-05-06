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

import com.twitter.algebird.Monoid
import com.twitter.bijection.{ Injection, NumericInjections }
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.algebra.{ ConvertedStore, MergeableStore }
import com.twitter.util.{ Duration, Future }
import org.jboss.netty.buffer.ChannelBuffer

/**
 * 
 * @author Doug Tangren
 */

object RedisLongStore extends NumericInjections {
   /** redis stores numerics as strings
    *  so we have to encode/decode them as such
    *  http://redis.io/topics/data-types-intro
    */
  private [redis] implicit val LongInjection =
    long2String.andThen(RedisStringStore.StringInjection)

  def apply(client: Client, ttl: Option[Duration] = RedisStore.Default.TTL) =
    new RedisLongStore(RedisStore(client, ttl))
}
import RedisLongStore._

/**
 * A MergableStore backed by redis which stores Long values.
 * Values are merged with an incrBy operation.
 */
class RedisLongStore(underlying: RedisStore)
  extends ConvertedStore[ChannelBuffer, ChannelBuffer, ChannelBuffer, Long](underlying)(identity)
     with MergeableStore[ChannelBuffer, Long] {
  val monoid = implicitly[Monoid[Long]]
  override def merge(kv: (ChannelBuffer, Long)): Future[Unit] =
    underlying.client.incrBy(kv._1, kv._2).unit
}
