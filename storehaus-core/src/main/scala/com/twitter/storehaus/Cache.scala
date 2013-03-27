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

/**
  * Cache trait for use with Storehaus stores.
  */

object Cache {
  def touch[K, V, T <: Cache[K, V]](cache: T, k: K)(fn: K => V): T = {
    if (cache.contains(k))
      cache.hit(k)
    else
      cache.miss(k -> fn(k))
  }

  def multiTouch[K1 <: K, K, V, T <: Cache[K, V]](cache: T, ks: Set[K1])(fn: Set[K1] => Map[K1, V]): T = {
    val (hits, misses) =
      ks.map { k => k -> cache.contains(k) }
        .partition { _._2 }
    val hitCache = hits.foldLeft(cache) { case (acc, (k, _)) => acc.hit(k) }
    fn(misses.map { _._1 }.toSet).foldLeft(hitCache) { _.miss(_) }
  }
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
  def hit(k: K): this.type

  /* Writes the supplied pair into the cache. */
  def miss(kv: (K, V)): this.type

  /* Removes the supplied key from the cache. */
  def evict(k: K): this.type

  /* re-initializes the cache using the supplied map as a seed. */
  def seed[T <: K](seed: Map[T, V]): this.type

  /**
    * Touches the cache with the supplied key. If the key is present
    * in the cache, the cache calls "hit" on the key. If the key is
    * missing, the cache adds fn(k) to itself.
    */
  def touch(k: K)(fn: K => V): this.type = Cache.touch[K, V, this.type](this, k)(fn)

  /**
    * The same as touch for multiple keys.
    */
  def multiTouch[K1 <: K](ks: Set[K1])(fn: Set[K1] => Map[K1, V]) =
    Cache.multiTouch[K1, K, V, this.type](this, ks)(fn)
}
