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

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object StoreProperties extends Properties("Store") {
  // Adds a bunch of items and removes them and sees if they are absent
  def storeTest[K, V](store: Store[K, V])
    (implicit arbk: Arbitrary[K], arbv: Arbitrary[V]) =
    forAll { (examples: List[(K,V)]) =>
      examples.forall { (kv) =>
        store.put((kv._1, Some(kv._2))).get // force the future
        store.get(kv._1).get == Some(kv._2)
      } && examples.forall { kv =>
        store.put((kv._1, None)).get // force the future
        store.get(kv._1).get == None
      }
    }

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
