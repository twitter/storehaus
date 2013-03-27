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

package com.twitter.storehaus

import scala.collection.SortedMap

/**
  * Cache trait for use with Storehaus stores.
  *
  * Inspired by clojure's core.cache:
  * https://github.com/clojure/core.cache/blob/master/src/main/clojure/clojure/core/cache.clj
  *
  * @author Sam Ritchie
  */

object Cache {
  def basic[K, V] = new BasicCache(Map.empty[K, V])
  def lru[K, V](maxSize: Long) =
    new LRUCache(maxSize, 0, Map.empty[K, (Long, V)], SortedMap.empty[Long, K])
}

trait Cache[K, V] {
  /**
    * Returns an option containing the value stored for the supplied
    * key or None if the key is not present in the cache. (A call to
    * get does NOT mutate the cache.)
    */
  def get(k: K): Option[V]

  /**
    * Returns true if the cache contains a value for the supplied key,
    * false otherwise.
    */
  def contains(k: K): Boolean

  /* Promotes the supplied key within the cache. */
  def hit(k: K): Cache[K, V]

  /* Writes the supplied pair into the cache. */
  def put(kv: (K, V)): Cache[K, V]

  /* Removes the supplied key from the cache. */
  def evict(k: K): Cache[K, V]

  /* re-initializes the cache using the supplied map as a seed. */
  def seed(seed: Map[K, V]): Cache[K, V]

  /**
    * Touches the cache with the supplied key. If the key is present
    * in the cache, the cache calls "hit" on the key. If the key is
    * missing, the cache adds fn(k) to itself.
    */
  def touch(k: K)(fn: K => V): Cache[K, V] =
    if (contains(k))
      hit(k)
    else
      put(k -> fn(k))

  /**
    * The same as touch for multiple keys.
    */
  def multiTouch(ks: Set[K])(fn: Set[K] => Map[K, V]): Cache[K, V] = {
    val (hits, misses) =
      ks.map { k => k -> contains(k) }
        .partition { _._2 }
    val hitCache = hits.foldLeft(this) { case (acc, (k, _)) => acc.hit(k) }
    fn(misses.map { _._1 }.toSet).foldLeft(hitCache) { _.put(_) }
  }
}
