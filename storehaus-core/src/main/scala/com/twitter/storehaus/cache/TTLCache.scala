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

import com.twitter.util.Duration

/**
 * Immutable implementation of a *T*ime *T*o *L*ive cache.
 *
 * Every value placed into the cache will be assigned a time
 * (generated via the supplied clock function). After the supplied
 * time-to-live Duration has passed, placing any new pair into the
 * cache will evict all expired keys.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

class TTLCache[K, V](ttl: Duration, cache: Map[K, V], keyToMillis: Map[K, Long])(clock: () => Long) extends Cache[K, V] {
  val ttlInMillis = ttl.inMillis

  override def get(k: K): Option[V] = if (contains(k)) cache.get(k) else None
  override def contains(k: K): Boolean =
    keyToMillis.get(k)
      .exists { timestamp => (clock() - timestamp) < ttlInMillis }

  override def hit(k: K): Cache[K, V] = this

  protected def toRemove(currentMillis: Long): Set[K] =
    keyToMillis.filter {
      case (_, timestamp) =>
        timestamp < (currentMillis - ttlInMillis)
    }.keySet

  override def put(kv: (K, V)) = {
    val now = clock()
    val killKeys = toRemove(now)
    val newCache = cache -- killKeys + kv
    val newKeyToMillis = keyToMillis -- killKeys + (kv._1 -> now)
    (killKeys, new TTLCache(ttl, newCache, newKeyToMillis)(clock))
  }

  override def evict(k: K): (Option[V], Cache[K, V]) =
    cache.get(k).map { v =>
      (Some(v), new TTLCache(ttl, cache - k, keyToMillis - k)(clock))
    }.getOrElse((None, this))

  override def empty = new TTLCache(ttl, Map.empty[K, V], Map.empty[K, Long])(clock)
  override def iterator = cache.iterator
  override def toMap = cache
}
