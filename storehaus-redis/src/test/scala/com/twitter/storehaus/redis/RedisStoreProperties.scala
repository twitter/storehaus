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

import com.twitter.bijection.Bijection
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer }
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.redis._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import org.scalacheck.{ Arbitrary, Gen, Prop, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import scala.util.control.Exception.allCatch

object RedisStoreProperties extends Properties("RedisStore") {

  object Strs extends Bijection[ChannelBuffer, String] {
    override def apply(cb: ChannelBuffer) =
      CBToString(cb)
    override def invert(str: String) =
      StringToChannelBuffer(str)
  }

  
  val validPairs = Arbitrary.arbitrary[List[(String, Option[String])]] suchThat {
    case Nil => false
    case xs => xs.forall {
      case (k, Some(s)) => !k.isEmpty && !s.isEmpty
      case (k, None)    => !k.isEmpty
    }
  }

  def baseTest(store: RedisStore)
    (put: (RedisStore, List[(String, Option[String])]) => Unit) =
    forAll(validPairs) { (examples: List[(String, Option[String])]) =>
      println(examples)
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        store.get(Strs.invert(k)).get.map(Strs.apply) == optV
      }
    }

  def putStoreTest(store: RedisStore) =
    baseTest(store) { (s, pairs) =>
      pairs.foreach { case (k, v) => s.put((Strs.invert(k), v.map(Strs.invert))).get }
    }

  def multiPutStoreTest(store: RedisStore) =
    baseTest(store) { (s, pairs) =>
      FutureOps.mapCollect(s.multiPut(pairs.map({ case (k, v) => (Strs.invert(k), v.map(Strs.invert)) }).toMap)).get
    }

  def storeTest(store: RedisStore) =
    putStoreTest(store)// && multiPutStoreTest(store)

  property("RedisStore test") =
    withStore(storeTest(_))

  private def withStore[T](f: RedisStore => T): T = {
    val cli = Client("localhost:6379")
    cli.flushDB()
    val store = RedisStore(cli)
    val result = f(store)
    store.close
    result
  }
}
