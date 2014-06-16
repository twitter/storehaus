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

import com.twitter.storehaus.{Store, EagerWriteThroughCacheStore }
import com.twitter.storehaus.cache.MutableCache
import com.twitter.util.Future
import com.twitter.storehaus.cache.{MutableCache, HeavyHittersPercent, WriteOperationUpdateFrequency, RollOverFrequencyMS, HHFilteredCache}

object HHFilteredStore {
  def buildStore[K, V](store: Store[K, V], cache: MutableCache[K, Future[Option[V]]], hhPct: HeavyHittersPercent,
                          writeUpdateFreq: WriteOperationUpdateFrequency, rolloverFreq: RollOverFrequencyMS): Store[K, V] = {
    val filteredCacheStore = new HHFilteredCache(cache, hhPct, writeUpdateFreq, rolloverFreq)
    new EagerWriteThroughCacheStore[K, V](store, filteredCacheStore)
  }
}
