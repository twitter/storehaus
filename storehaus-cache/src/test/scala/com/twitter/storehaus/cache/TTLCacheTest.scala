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

import org.scalatest.{Matchers, WordSpec}
import com.twitter.util.Duration

class TTLCacheTest extends WordSpec with Matchers {
  val ttlMS = 600
  val cache = Cache.ttl[String, Int](Duration.fromMilliseconds(ttlMS))

  "TTLCache exhibits proper TTL-ness" in {
    val abCache = cache.putClocked("a" -> 1)._2.putClocked("b" -> 2)._2
    abCache.toNonExpiredMap should equal(Map("a" -> 1, "b" -> 2))
    Thread.sleep(ttlMS)
    abCache.putClocked("c" -> 3)._2.toNonExpiredMap should equal(Map("c" -> 3))
  }

  "TTLCache does not return an expired value" in {
    val withV = cache.putClocked("a" -> 10)._2
    withV.getNonExpired("a") should equal(Some(10))
    Thread.sleep(ttlMS)
    withV.getNonExpired("a") should equal(None)
  }
}
