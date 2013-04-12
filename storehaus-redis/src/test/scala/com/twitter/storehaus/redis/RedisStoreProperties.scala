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

import com.twitter.bijection.{ Bijection, Injection }
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.storehaus.{ FutureOps, Store }
import com.twitter.storehaus.algebra.ConvertedStore
import org.jboss.netty.buffer.ChannelBuffer
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import scala.util.control.Exception.allCatch

object RedisStoreProperties extends Properties("RedisStore")
  with CloseableCleanup[RedisStore] {

  object Strs extends Bijection[ChannelBuffer, String] {
    override def apply(cb: ChannelBuffer) =
      CBToString(cb)
    override def invert(str: String) =
      StringToChannelBuffer(str)
  }
  
  def validPairs(examples: List[(String, Option[String])]) =
    !examples.isEmpty && examples.forall {
      case (k, v) if (k.isEmpty || v.filter(_.isEmpty).isDefined) => false
      case _ => true
    }

  def baseTest[K : Arbitrary, V: Arbitrary: Equiv](store: Store[K, V], implication: List[(K, Option[V])] => Boolean)
    (put: (Store[K, V], List[(K, Option[V])]) => Unit) =
    forAll { (examples: List[(K, Option[V])]) =>
      implication(examples) ==> {
        put(store, examples)
        examples.toMap.forall { case (k, optV) =>
          store.get(k).get == optV
        }
      }
    }

  def putStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V], implication: List[(K, Option[V])] => Boolean) =
    baseTest(store, implication) { (s, pairs) =>
      pairs.foreach { case (k, v) => s.put((k, v)).get }
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V], implication: List[(K, Option[V])] => Boolean) =
    baseTest(store, implication) { (s, pairs) =>
      FutureOps.mapCollect(s.multiPut(pairs.toMap)).get
    }

  def storeTest(store: Store[String, String]) =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val closeable: Store[String, String] = {
    val client = Client("localhost:6379")
    client.flushDB() // clean slate
    new ConvertedStore(RedisStore(client))(StringToChannelBuffer(_: String))(
      new Injection[String, ChannelBuffer] {
        def apply(a: String): ChannelBuffer = StringToChannelBuffer(a)
        def invert(b: ChannelBuffer): Option[String] = allCatch.opt(CBToString(b))
      })
  }

  property("RedisStore test") =
    storeTest(closeable)
}
