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

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.bijection.twitter_util.UtilBijections.Shared.byteArrayBufBijection
import com.twitter.finagle.redis.Client
import com.twitter.io.Buf
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{Duration, Future}

/**
 * Uses a byte array to buf injection to convert from Store[Buf, Buf] to Store[Buf, Long].
 *
 * The injection: Shared.byteArrayBufBijection does a fully copy of the underlying Buf instance.
 *
 * @author Doug Tangren
 */

object RedisLongStore {
   /** redis stores numerics as strings
    *  so we have to encode/decode them as such
    *  http://redis.io/topics/data-types-intro
    */
  private [redis] implicit val LongInjection =
    Injection.connect[Long, String, Array[Byte], Buf]

  def apply(client: Client, ttl: Option[Duration] = RedisStore.Default.TTL): RedisLongStore =
    new RedisLongStore(RedisStore(client, ttl))
}
import RedisLongStore._

/**
 * A MergableStore backed by redis which stores Long values.
 * Values are merged with an incrBy operation.
 */
class RedisLongStore(underlying: RedisStore)
  extends ConvertedStore[Buf, Buf, Buf, Long](underlying)(identity)
     with MergeableStore[Buf, Long] {
  val semigroup = implicitly[Semigroup[Long]]
  override def merge(kv: (Buf, Long)): Future[Option[Long]] =
    underlying.client.incrBy(kv._1, kv._2).map(v => Some(v - kv._2)) // redis returns the result
}
