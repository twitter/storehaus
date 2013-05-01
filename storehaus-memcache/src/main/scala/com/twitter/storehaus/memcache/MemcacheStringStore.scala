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

import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.util.Duration
import com.twitter.finagle.memcached.Client
import com.twitter.storehaus.algebra.{ ConvertedStore, MergeableStore }
import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }
import org.jboss.netty.util.CharsetUtil
import scala.util.control.Exception.allCatch

/**
 *  @author Doug Tangren
 */
object MemcacheStringStore {
  // TODO: move this into bijection-netty
  private [memcache] implicit object ByteArrayInjection
   extends Injection[Array[Byte],ChannelBuffer] {
    def apply(ary: Array[Byte]) = ChannelBuffers.wrappedBuffer(ary)
    def invert(buf: ChannelBuffer) = allCatch.opt(buf.array)
  }
  private [memcache] implicit val StringInjection =
    Injection.connect[String, Array[Byte], ChannelBuffer]

  def apply(client: Client, ttl: Duration = MemcacheStore.DEFAULT_TTL, flag: Int = MemcacheStore.DEFAULT_FLAG) =
    new MemcacheStringStore(MemcacheStore(client, ttl, flag))
}
import MemcacheStringStore._

/** A MergeableStore for String values backed by memcache */
class MemcacheStringStore(underlying: MemcacheStore) 
  extends ConvertedStore[String, String, ChannelBuffer, String](underlying)(identity)
  with MergeableStore[String, String] {

  val monoid = implicitly[Monoid[String]]

  /** Merges a key by appending a String value. This has no affect
   *  if there was no previous value for the provided key. */
  override def merge(kv: (String, String)) =
    stringStore.append(kv._1, kv._2).unit

  private val stringStore = underlying.client.withStrings
}
