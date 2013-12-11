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

import com.twitter.bijection.Injection
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.storehaus.{ FutureOps, Store }
import com.twitter.storehaus.algebra.ConvertedStore
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffer
import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalacheck.Prop._
import scala.util.Try

object RedisStoreProperties extends Properties("RedisStore")
  with CloseableCleanup[Store[String, String]]
  with DefaultRedisClient {

  def validPairs: Gen[List[(String, Option[String])]] =
    NonEmpty.Pairing.alphaStrs()

  def baseTest[K : Arbitrary, V: Arbitrary: Equiv](store: Store[K, V], validPairs: Gen[List[(K, Option[V])]])
    (put: (Store[K, V], List[(K, Option[V])]) => Unit) =
    forAll(validPairs) { (examples: List[(K, Option[V])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        val res = Await.result(store.get(k))
        Equiv[Option[V]].equiv(res, optV)
      }
    }

  def putStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V],validPairs: Gen[List[(K, Option[V])]]) =
    baseTest(store, validPairs) { (s, pairs) =>
      pairs.foreach { case (k, v) => Await.result(s.put((k, v))) }
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V], validPairs: Gen[List[(K, Option[V])]]) =
    baseTest(store, validPairs) { (s, pairs) =>
      Await.result(FutureOps.mapCollect(s.multiPut(pairs.toMap)))
    }

  def storeTest(store: Store[String, String]) =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  implicit def strToCb =
    Injection.build(StringToChannelBuffer(_: String))(
      (b: ChannelBuffer) => Try(CBToString(b)))

  val closeable =
    RedisStore(client).convert(StringToChannelBuffer(_: String))

  property("RedisStore test") = storeTest(closeable)
}
