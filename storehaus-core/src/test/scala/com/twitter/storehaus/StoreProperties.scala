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

package com.twitter.storehaus

import com.twitter.util.Future

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object StoreProperties extends Properties("Store") {
  def baseTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V])
    (put: (Store[K, V], List[(K, Option[V])]) => Unit) =
    forAll { (examples: List[(K, Option[V])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
          Equiv[Option[V]].equiv(store.get(k).get, optV)
      }
    }

  def putStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V]) =
    baseTest(store) { (s, pairs) =>
      pairs.foreach { s.put(_).get }
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V]) =
    baseTest(store) { (s, pairs) =>
      FutureOps.mapCollect(s.multiPut(pairs.toMap)).get
    }

  def storeTest[K: Arbitrary, V: Arbitrary: Equiv](store: Store[K, V]) =
    putStoreTest(store) && multiPutStoreTest(store)

  property("multiGet returns Some(Future(None)) for missing keys") =
    forAll { (m: Map[String, Int]) =>
      val keys = m.keySet
      val expanded: Set[String] = keys ++ (keys map { _ + "suffix!" })
      val ms = ReadableStore.fromMap(m)
      val retM = ms.multiGet(expanded)
      expanded forall { s: String =>
        ms.get(s).get == retM(s).get
      }
    }

  property("Map wraps store works") = forAll { (m: Map[String, Int]) =>
    val ms = ReadableStore.fromMap(m)
    val retM = ms.multiGet(m.keySet)
    (retM.forall { kv: (String, Future[Option[Int]]) =>
        (ms.get(kv._1).get == kv._2.get) && (m.get(kv._1) == kv._2.get)
      }) && (m.keySet.forall { k => (ms.get(k).get == m.get(k)) })
  }

  property("ConcurrentHashMapStore test") = storeTest(new ConcurrentHashMapStore[String,Int]())

  property("LRUStore test") = storeTest(Store.lru[String,Int](100000))

  property("Or works as expected") = forAll { (m1: Map[String, Int], m2: Map[String, Int]) =>
    val orRO = ReadableStore.first(Seq(ReadableStore.fromMap(m1), ReadableStore.fromMap(m2)))
   (m1.keySet ++ m2.keySet).forall { k =>
     (orRO.get(k).get == m1.get(k)) ||
       (orRO.get(k).get == m2.get(k))
   }
  }
}
