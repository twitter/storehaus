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

package com.twitter.storehaus.algebra

import com.twitter.algebird.Monoid
import com.twitter.storehaus.{BaseProperties, MapStore, Store}
import org.scalacheck.{Arbitrary, Properties}
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import org.scalacheck.Properties

object AggregatingStoreLaws extends Properties("AggregatingStore") {
  // Adds a bunch of items and removes them and sees if they are absent
  def aggregatingStoreTest[StoreType <: Store[StoreType, K, V], K: Arbitrary, V: Arbitrary: Monoid]
  (store: StoreType) =
    forAll { examples: List[(K, V)] =>
      val adds = examples.foldLeft((store, true)) { (old, pair) =>
        val (store, truthAcc) = old
        val (k, newV) = pair
        val summedV = Monoid.plus(store.get(k).get, Some(newV))
          .flatMap { Monoid.nonZeroOption(_) }
        val nextStore = (store + pair).get
        val nextGood = nextStore.get(k).get == summedV
        (nextStore, truthAcc && nextGood)
      }
      examples.foldLeft(adds) { (old, kv) =>
        val next = (old._1 - (kv._1)).get
        val nextGood = (next.get(kv._1).get == None)
        (next, nextGood && old._2)
      }._2
    }

  property("AggregatingStore should accept and delete many keys") = {
    val mapStore = new MapStore[String, Int]()
    type StoreType = AggregatingStore[MapStore[String, Int], String, Int]
    aggregatingStoreTest[StoreType, String, Int](AggregatingStore(mapStore))
  }
}
