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

import com.twitter.util.{ Await, Future }

import org.scalacheck.{Prop, Arbitrary, Properties}
import org.scalacheck.Prop._

object StoreProperties extends Properties("Store") {
  def baseTest[K: Arbitrary, V: Arbitrary: Equiv](storeIn: => Store[K, V])
  (put: (Store[K, V], List[(K, Option[V])]) => Map[K, Option[V]]): Prop = {
    forAll { (examples: List[(K, Option[V])]) =>
      lazy val store = storeIn
      put(store, examples).forall { case (k, optV) =>
        Equiv[Option[V]].equiv(Await.result(store.get(k)), optV)
      }
    }
  }

  def putStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: => Store[K, V]): Prop =
    baseTest(store) { (s, pairs) =>
      Await.result(pairs.foldLeft(Future.Unit) { (fOld, p) => fOld.flatMap { _ => s.put(p) } })
      pairs.toMap
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary: Equiv](store: => Store[K, V]): Prop =
    baseTest(store) { (s, pairs) =>
      Await.result(FutureOps.mapCollect(s.multiPut(pairs.toMap)))
      pairs.toMap
    }

  def storeTest[K: Arbitrary, V: Arbitrary: Equiv](store: => Store[K, V]): Prop =
    putStoreTest(store) && multiPutStoreTest(store)

  def sparseStoreTest[K: Arbitrary, V: Arbitrary: Equiv](
      norm: Option[V] => Option[V])(store: => Store[K, V]): Prop =
    baseTest(store) { (s, pairs) =>
      Await.result(pairs.foldLeft(Future.Unit) { (fOld, p) => fOld.flatMap { _ => s.put(p) } })
      pairs.toMap.mapValues(norm)
    } && baseTest(store) { (s, pairs) =>
      Await.result(FutureOps.mapCollect(s.multiPut(pairs.toMap)))
      pairs.toMap.mapValues(norm)
    }

  property("ConcurrentHashMapStore test") =
    storeTest(new ConcurrentHashMapStore[String, Int]())

  property("Or works as expected") = forAll { (m1: Map[String, Int], m2: Map[String, Int]) =>
    val orRO = ReadableStore.first(Seq(ReadableStore.fromMap(m1), ReadableStore.fromMap(m2)))
   (m1.keySet ++ m2.keySet).forall { k =>
     (Await.result(orRO.get(k)) == m1.get(k)) ||
       (Await.result(orRO.get(k)) == m2.get(k))
   }
  }
}
