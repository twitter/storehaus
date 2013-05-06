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
import com.twitter.bijection.Injection
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.storehaus.algebra.{ ConvertedStore, MergeableStore }
import com.twitter.util.{ Duration, Future }
import org.jboss.netty.buffer.ChannelBuffer
import scala.util.control.Exception.allCatch

/**
 * 
 *  @author Doug Tangren
 */

object RedisStringStore {
  private [redis] implicit object StringInjection
   extends Injection[String, ChannelBuffer] {
    def apply(a: String): ChannelBuffer = StringToChannelBuffer(a)
    def invert(b: ChannelBuffer): Option[String] = allCatch.opt(CBToString(b))
  }

  def apply(client: Client, ttl: Option[Duration] = RedisStore.Default.TTL) =
    new RedisStringStore(RedisStore(client, ttl))
}
import RedisStringStore._

/**
 * A MergableStore backed by redis which stores String values
 * Values are merged by with an append operation.
 */
class RedisStringStore(underlying: RedisStore)
  extends ConvertedStore[ChannelBuffer, ChannelBuffer, ChannelBuffer, String](underlying)(identity)
     with MergeableStore[ChannelBuffer, String] {
  val monoid = implicitly[Monoid[String]]
  override def merge(kv: (ChannelBuffer, String)): Future[Unit] =
    underlying.client.append(kv._1, kv._2).unit
}

                              
