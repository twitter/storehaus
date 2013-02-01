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
import org.scalacheck.Prop._

trait BaseProperties {
  // Adds a bunch of items and removes them and sees if they are absent
  def storeTest[StoreType <: Store[StoreType, K, V], K: Arbitrary, V: Arbitrary](store: StoreType) =
    forAll { (examples: List[(K, V)]) =>
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
}
