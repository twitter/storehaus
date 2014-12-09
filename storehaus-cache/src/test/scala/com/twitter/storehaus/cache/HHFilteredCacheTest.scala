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

package com.twitter.storehaus.cache

import org.scalatest.{ WordSpec, Matchers }

class HHFilteredCacheTest extends WordSpec with Matchers {
  def checkCache[K, V](pairs: Seq[(K, V)], m: Map[K, V])(implicit cache: MutableCache[K, V]) = {
    pairs.foldLeft(cache)(_ += _)
    val res = cache.iterator.toMap
    cache.clear
    res shouldBe m
  }

  "HHFilteredCache" should {
    "work properly" in {
      val backingCache = MutableCache.fromJMap[String, Int](new java.util.LinkedHashMap[String, Int])

      implicit val cache = new HHFilteredCache[String, Int](backingCache, HeavyHittersPercent(0.5f), WriteOperationUpdateFrequency(1), RollOverFrequencyMS(10000000L))

      checkCache(
        Seq("a" -> 1, "b" -> 2),
        Map("a" -> 1, "b" -> 2)
      )

      // Ensures the previous clear operations are running
      // The output == input
      checkCache(
        Seq("c" -> 1, "d" -> 2),
        Map("c" -> 1, "d" -> 2)
      )

      // Nothing above the 0.5 HH theshold
      checkCache(
        Seq("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 3, "e" -> 3, "a" -> 1),
        Map()
      )

      // Only b should be above the HH threshold
      checkCache(
        Seq("a" -> 1, "b" -> 2, "b" -> 3, "c" -> 1, "b" -> 3, "a" -> 1),
        Map("b" -> 3)
      )
    }
  }
}
