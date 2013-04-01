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

import org.specs._

class LRUCacheTest extends Specification {
  def checkCache[K, V](pairs: Seq[(K, V)], m: Map[K, V])(implicit cache: Cache[K, V]) =
    pairs.foldLeft(cache)(_ + _).toMap must be_==(m)

  "LRUCache works properly with threshold 2" in {
    implicit val cache = Cache.lru[String, Int](2)
    checkCache(
      Seq("a" -> 1, "b" -> 2),
      Map("a" -> 1, "b" -> 2)
    )
    checkCache(
      Seq("a" -> 1, "b" -> 2, "c" -> 3),
      Map("b" -> 2, "c" -> 3)
    )
    checkCache(
      Seq("a" -> 1, "b" -> 2, "b" -> 3),
      Map("a" -> 1, "b" -> 3)
    )
    ((cache + ("a" -> 1) + ("b" -> 2)).hit("a") + ("c" -> 3)).toMap
      .must(be_==(Map("a" -> 1, "c" -> 3)))
  }
}
