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

import scala.collection.immutable.Queue

/**
  * Immutable implementation of a *F*irst *I*n, *F*irst *O*ut cache.
  */

class FIFOCache[K, V](maxSize: Long, m: Map[K, V], queue: Queue[K]) extends Cache[K, V] {
  override def get(k: K): Option[V] = m.get(k)
  override def contains(k: K): Boolean = m.contains(k)
  override def hit(k: K): Cache[K, V] = this
  override def put(kv: (K, V)): Cache[K, V] = {
    val (newM, newQueue) =
      if (m.size >= maxSize && !queue.isEmpty) {
        val (poppedKey, smaller) = queue.dequeue
        (m - poppedKey, smaller)
      } else
        (m, queue)
    new FIFOCache(maxSize, newM + kv, newQueue :+ kv._1)
  }
  override def evict(k: K): (Option[V], Cache[K, V]) =
    m.get(k).map { v =>
      (Some(v), new FIFOCache(maxSize, m - k, queue.filterNot { _ == k }))
    }.getOrElse((None, this))
}
