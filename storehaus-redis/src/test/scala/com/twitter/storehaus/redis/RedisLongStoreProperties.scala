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

object RedisLongStoreProperties extends Properties("RedisLongStore")
  with CloseableCleanup[RedisLongStore] {
  
  def validPairs(examples: List[(String, Option[Long])]) =
    !examples.isEmpty && examples.forall {
      case (k, v) if (k.isEmpty) => false
      case _ => true
    }

  def baseTest(store: RedisLongStore)
    (put: (RedisLongStore, List[(String, Option[Long])]) => Unit) =
    forAll { (examples: List[(String, Option[Long])]) =>
      validPairs(examples) ==> {
        put(store, examples)
        examples.toMap.forall { case (k, optV) =>
          store.get(Strs.invert(k)).get == optV
        }
      }
    }

  def putStoreTest(store: RedisLongStore) =
    baseTest(store) { (s, pairs) =>
      pairs.foreach { case (k, v) => s.put((Strs.invert(k), v)).get }
    }

  def multiPutStoreTest(store: RedisLongStore) =
    baseTest(store) { (s, pairs) =>
      FutureOps.mapCollect(s.multiPut(pairs.map({ case (k, v) => (Strs.invert(k), v) }).toMap)).get
    }

  def storeTest(store: RedisLongStore) =
    putStoreTest(store) && multiPutStoreTest(store)

  property("RedisLongStore test") =
    storeTest(closeable)

  val closeable = {
    val client = Client("localhost:6379")
    client.flushDB() // clean slate
    val rs = RedisLongStore(client)
    rs
  }
}
