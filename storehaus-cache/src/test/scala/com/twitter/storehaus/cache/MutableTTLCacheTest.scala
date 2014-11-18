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
import com.twitter.util.Duration

class MutableTTLCacheTest extends Specification {

  "TTLCache exhibits proper TTL-ness" in {
    val ttl: Duration = Duration.fromMilliseconds(500)
    val cache = MutableCache.ttl[String, Int](ttl, 100)
    cache += ("a" -> 1)
    cache += ("b" -> 2)
    cache.toNonExpiredMap must be_==(Map("a" -> 1, "b" -> 2))
    Thread.sleep(ttl.inMilliseconds)
    cache += ("c" -> 3)
    cache.toNonExpiredMap must be_==(Map("c" -> 3))
  }

  "TTLCache does not return an expired value" in {
    val ttl: Duration = Duration.fromMilliseconds(500)
    val cache = MutableCache.ttl[String, Int](ttl, 100)
    cache += ("a" -> 10)
    cache.get("a") must be_==(Some(10))
    Thread.sleep(ttl.inMilliseconds)
    cache.get("a") must be_==(None)
  }

  "TTL-ness is honored in getOrElseUpdate" in {
    val ttl: Duration = Duration.fromMilliseconds(500)
    val cache = MutableTTLCache[String, Int](ttl = ttl, capacity = 100)
    cache.getOrElseUpdate("a", 1) mustEqual 1
    cache.get("a") must beSome[Int](1)
    Thread.sleep(200)
    cache.getOrElseUpdate("a", 2) mustEqual 1
    Thread.sleep(300)
    cache.getOrElseUpdate("a", 3) mustEqual 3
    cache.get("a") must beSome[Int](3)
  }
}
