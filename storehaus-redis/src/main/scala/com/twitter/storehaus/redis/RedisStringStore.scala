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
import com.twitter.util.Time
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.storehaus.algebra.MergeableStore
import org.jboss.netty.buffer.ChannelBuffer

/**
 * 
 *  @author Doug Tangren
 */

object RedisStringStore {
  implicit object StringChannelBuffered extends ChannelBuffered[String] {
    override def apply(cb: ChannelBuffer): String = CBToString(cb)
    override def invert(in: String) = StringToChannelBuffer(in)
  }
}
import RedisStringStore._

class RedisStringStore(client: Client, ttl: Option[Time])
 extends RedisStore[String](client, ttl)
    with MergeableStore[ChannelBuffer, String] {
  val monoid = implicitly[Monoid[String]]
  override def merge(kv: (ChannelBuffer, String)) = client.append(kv._1, kv._2).unit
}

