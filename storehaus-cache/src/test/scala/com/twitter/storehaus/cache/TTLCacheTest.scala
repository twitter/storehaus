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

class TTLCacheTest extends Specification {
  val cache = Cache.ttl[String, Int](200)

  "TTLCache exhibits proper TTL-ness" in {
    val abCache = cache.putClocked("a" -> 1)._2.putClocked("b" -> 2)._2
    abCache.toNonExpiredMap must be_==(Map("a" -> 1, "b" -> 2))
    Thread.sleep(300)
    (abCache.putClocked("c" -> 3)._2).toNonExpiredMap must be_==(Map("c" -> 3))
  }

  "TTLCache does not return an expired value" in {
    val withV = cache.putClocked("a" -> 10)._2
    withV.getNonExpired("a") must be_==(Some(10))
    Thread.sleep(300)
    withV.getNonExpired("a") must be_==(None)
  }
}
