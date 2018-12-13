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

import org.scalacheck.{Prop, Arbitrary, Properties}
import org.scalacheck.Prop._
import com.twitter.util.Duration

object CacheProperties extends Properties("Cache") {
  /**
    * After adding in a bunch of key-value pairs, the set of keys
    * added is equivalent to the union of evicted keys and keys
    * present in the final cache.
    */
  def unionLaw[K: Arbitrary, V: Arbitrary](cache: Cache[K, V]): Prop =
    forAll { (pairs: List[(K, V)]) =>
      val (evictedSet, newCache) =
        pairs.foldLeft(Set.empty[K], cache) { (acc, pair) =>
          val (evictedKeyAcc, cacheAcc) = acc
          val (newEvictions, newCache) = cacheAcc.put(pair)
          (evictedKeyAcc ++ newEvictions, newCache)
        }
      (newCache.toMap.keySet ++ evictedSet) == pairs.map { _._1 }.toSet
    }

  /**
    * Once a pair is placed into an immutable cache, it'll always be
    * evicted immediately or available via get.
    */
  def presentLaw[K: Arbitrary, V: Arbitrary](cache: Cache[K, V]): Prop =
    forAll { (pairs: List[(K, V)]) =>
      pairs.forall { case pair@(k, v) =>
        val (evictedKeys, newCache) = cache.put(pair)
        newCache.contains(k) || evictedKeys.contains(k)
      }
    }

  def cacheLaws[K: Arbitrary, V: Arbitrary](cache: Cache[K, V]): Prop =
    unionLaw(cache) && presentLaw(cache)

  property("MapCache obeys the cache laws") = cacheLaws(MapCache.empty[String, Int])

  property("MapCache never evicts") =
    forAll { pairs: List[(Int, String)] =>
      pairs.foldLeft(MapCache.empty[Int, String]: Cache[Int, String])(_ + _)
        .toMap == pairs.toMap[Int, String]
    }

  property("LRUCache obeys the cache laws") = cacheLaws(LRUCache[String, Int](10))
  property("LIRSCache obeys the cache laws") = cacheLaws(LIRSCache[String, Int](10, .8))
  property("TTLCache obeys the cache laws") = cacheLaws(TTLCache[String, Int](Duration.fromMilliseconds(10)))
}
