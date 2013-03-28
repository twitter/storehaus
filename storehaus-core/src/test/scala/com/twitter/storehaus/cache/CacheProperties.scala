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

import com.twitter.storehaus.Store
import com.twitter.util.Future
import com.twitter.conversions.time._

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object CacheProperties extends Properties("Cache") {
  def cacheLaws[K: Arbitrary, V: Arbitrary](cache: Cache[K, V]) =
    forAll { (pairs: List[(K, V)]) =>
      val (evictedSet, newCache) =
        pairs.foldLeft(Set.empty[K], cache) { (acc, pair) =>
          val (evictedKeyAcc, cacheAcc) = acc
          val (newEvictions, newCache) = cacheAcc.put(pair)
          (evictedKeyAcc ++ newEvictions, newCache)
        }
      (newCache.toMap.keySet ++ evictedSet) == pairs.map { _._1 }.toSet
    }

  property("MapCache obeys the cache laws") = cacheLaws(Cache.fromMap[String,Int]())

  property("MapCache never evicts") =
    forAll { pairs: List[(Int, String)] =>
      pairs.foldLeft(Cache.fromMap[Int, String](): Cache[Int, String])(_ + _)
        .toMap == pairs.toMap
    }

  property("LRUCache obeys the cache laws") = cacheLaws(Cache.lru[String,Int](10))
  property("TTLCache obeys the cache laws") =
    cacheLaws(Cache.ttl[String,Int](10.millis))
}
