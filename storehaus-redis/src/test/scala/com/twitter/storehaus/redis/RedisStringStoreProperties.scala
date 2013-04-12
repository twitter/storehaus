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
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.FutureOps
import org.scalacheck.Properties
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

import com.twitter.storehaus.redis.RedisStoreProperties.Strs

object RedisStringStoreProperties extends Properties("RedisStringStore")
  with CloseableCleanup[RedisStringStore] {
  
  def validPairs(examples: List[(String, Option[String])]) =
    !examples.isEmpty && examples.forall {
      case (k, v) if (k.isEmpty || v.filter(_.isEmpty).isDefined) => false
      case _ => true
    }

  def baseTest(store: RedisStringStore)
    (put: (RedisStringStore, List[(String, Option[String])]) => Unit) =
    forAll { (examples: List[(String, Option[String])]) =>
      validPairs(examples) ==> {
        put(store, examples)
        examples.toMap.forall { case (k, optV) =>
          store.get(Strs.invert(k)).get == optV
        }
      }
    }

  def putStoreTest(store: RedisStringStore) =
    baseTest(store) { (s, pairs) =>
      pairs.foreach { case (k, v) => s.put((Strs.invert(k), v)).get }
    }

  def multiPutStoreTest(store: RedisStringStore) =
    baseTest(store) { (s, pairs) =>
      FutureOps.mapCollect(s.multiPut(pairs.map({ case (k, v) => (Strs.invert(k), v) }).toMap)).get
    }

  def storeTest(store: RedisStringStore) =
    putStoreTest(store) && multiPutStoreTest(store)

  val closeable = {
    val client = Client("localhost:6379")
    client.flushDB() // clean slate
    val rs = RedisStringStore(client)
    rs
  }

  property("RedisStringStore test") =
    storeTest(closeable)
}
