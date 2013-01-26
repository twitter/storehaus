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
  def storeTest[StoreType<:Store[StoreType,K,V],K,V](store: StoreType)
    (implicit arbk: Arbitrary[K], arbv: Arbitrary[V]) =
    forAll { (examples: List[(K,V)]) =>
      val adds = examples.foldLeft((store, true)) { (old, kv) =>
        val next = (old._1 + (kv)).get
        val nextGood = (next.get(kv._1).get.get == kv._2)
        (next, nextGood && old._2)
      }
      examples.foldLeft(adds) { (old, kv) =>
        val next = (old._1 - (kv._1)).get
        val nextGood = (next.get(kv._1).get == None)
        (next, nextGood && old._2)
      }._2
    }

  property("multiGet returns Some(Future(None)) for missing keys") =
    forAll { (m: Map[String, Int]) =>
      val keys = m.keySet
      val expanded: Set[String] = keys ++ (keys map { _ + "suffix!" })
      val ms = new MapStore(m)
      (ms.multiGet(expanded) map { retM: Map[String, Future[Option[Int]]] =>
        expanded forall { s: String =>
          retM.get(s) match {
            case None => m.contains(s) == false
            case Some(f) => f.get == m.get(s)
          }
        }
      }).get
    }

  property("Map wraps store works") = forAll { (m: Map[String, Int]) =>
    val ms = new MapStore(m)
    (ms.keySet == m.keySet) &&
      (ms.multiGet(m.keySet).map { kv => (kv.mapValues { _.get }) == (m mapValues { Some(_) }) }.get) &&
      (m.keySet.map { k => (ms.get(k).get == m.get(k)) }.forall { x => x })
  }

  property("MapStore test") =
    storeTest[MapStore[String,Int],String,Int](new MapStore[String,Int]())

  property("ConcurrentHashMapStore test") =
    storeTest[ConcurrentHashMapStore[String,Int],String,Int](
      new ConcurrentHashMapStore[String,Int]())

  property("LRUStore test") =
    storeTest[LRUStore[String,Int],String,Int](LRUStore[String,Int](100000))

  property("Or works as expected") = forAll { (m1: Map[String, Int], m2: Map[String, Int]) =>
    val orRO = (new MapStore(m1)) or (new MapStore(m2))
   (m1.keySet ++ m2.keySet).forall { k =>
     (orRO.get(k).get == m1.get(k)) ||
       (orRO.get(k).get == m2.get(k))
   }
  }
}
