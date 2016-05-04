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
import com.twitter.bijection.NumericInjections
import com.twitter.bijection.twitter_util.UtilBijections
import com.twitter.util.{Duration, Future}
import com.twitter.finagle.memcached.Client
import com.twitter.io.Buf
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.Injection

import com.twitter.bijection.netty.ChannelBufferBijection

/**
 *  @author Doug Tangren
 */
object MemcacheLongStore {
  import UtilBijections.Owned.byteArrayBufBijection

  private implicit val cb2ary = ChannelBufferBijection
  // Long => String => ChannelBuffer <= String <= Long
  private[memcache] implicit val LongChannelBuffer: Injection[Long, ChannelBuffer] =
    Injection.connect[Long, String, Array[Byte], ChannelBuffer]

  // Long => String => Buf <= String <= Long
  private[memcache] implicit val LongBuf: Injection[Long, Buf] =
    Injection.connect[Long, String, Array[Byte], Buf]

  def apply(client: Client, ttl: Duration = MemcacheStore.DEFAULT_TTL, flag: Int = MemcacheStore.DEFAULT_FLAG) =
    new MemcacheLongStore(MemcacheStore(client, ttl, flag))
}
import MemcacheLongStore._

/** A MergeableStore for Long values backed by memcache */
class MemcacheLongStore(underlying: MemcacheStore)
  extends ConvertedStore[String, String, ChannelBuffer, Long](underlying)(identity)
  with MergeableStore[String, Long] {

  def semigroup = implicitly[Semigroup[Long]]

  /** Merges a key by incrementing by a Long value. */
  override def merge(kv: (String, Long)) = {
    val (k, v) = kv
    underlying.client.incr(k, v).flatMap {
      case Some(res) => Future.value(Some(res - v)) // value before
      case None => // memcache does not create on increment
        underlying
          .client
          .add(k, v.as[Buf])
          .flatMap { b => if(b) Future.value(None) else merge(kv) }
    }
  }
}

