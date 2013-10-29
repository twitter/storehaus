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

import org.specs2.mutable._

class MutableLRUCacheTest extends Specification {
  def freshCache = MutableLRUCache[String, Int](2)

  def checkCache(pairs: Seq[(String, Int)], results: Seq[Boolean]) = {
    val cache = freshCache
    pairs.foreach(cache += _)
    pairs.map { case (k, _) => cache.contains(k) } must be_==(results)
  }

  "MutableLRUCache works properly with threshold 2" in {
    // At capacity
    checkCache(
      Seq("a" -> 1, "b" -> 2),
      Seq(true, true)
    )
    // a is touched, so b and c are evicted
    checkCache(
      Seq("a" -> 1, "b" -> 2, "c" -> 3, "a" -> 1, "d" -> 4),
      Seq(true, false, false, true, true)
    )
  }
}
