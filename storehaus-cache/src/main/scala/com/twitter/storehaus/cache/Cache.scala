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

/**
 * Companion object to Cache. Contains a number of methods for
 * generating various cache implementations.
 */
object Cache {
  /**
   * Generate a Cache from the supplied Map. (Caveat emptor: this
   * will never evict keys!)
   */
  def fromMap[K, V](m: Map[K, V]) = MapCache(m)

  /**
   * Generate an LRUCache wrapping the supplied backingCache
   * configured with the supplied maxSize.
   */
  def lru[K, V](maxSize: Long, backingCache: Cache[K, (Long, V)] = MapCache.empty[K, (Long, V)]) =
    LRUCache(maxSize, backingCache)

  def ttl[K, V](ttlInMillis: Long, backingCache: Cache[K, (Long, V)] = MapCache.empty[K, (Long, V)]) =
    TTLCache(ttlInMillis, backingCache)

  def toMutable[K, V](cache: Cache[K, V])(exhaustFn: Set[K] => Unit): MutableCache[K, V] =
    new MutableFromImmutableCache(cache)(exhaustFn)
}

/**
 * Immutable Cache trait for use with Storehaus stores.
 *
 * Inspired by clojure's core.cache:
 * https://github.com/clojure/core.cache/blob/master/src/main/clojure/clojure/core/cache.clj
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

trait Cache[K, V] {
  /**
   * Returns an option containing the value stored for the supplied
   * key or None if the key is not present in the cache. (A call to
   * get should not change the underlying cache in any way.)
   */
  def get(k: K): Option[V]

  /* Returns a pair of Option[K] (representing a key possibly evicted by
   * new key-value pair) and a new cache containing the supplied
   * key-value pair. */
  def put(kv: (K, V)): (Set[K], Cache[K, V])

  /* Returns a new cache with the supplied Promotes the supplied key
   * within the cache. */
  def hit(k: K): Cache[K, V]

  /* Returns an option of the evicted value and the new cache state. */
  def evict(k: K): (Option[V], Cache[K, V])

  /**
   * Returns an iterator of all key-value pairs inside of this
   * cache.
   */
  def iterator: Iterator[(K, V)]

  /** Returns an empty version of this specific cache implementation. */
  def empty: Cache[K, V]

  /**
   * Returns true if the cache contains a value for the supplied key,
   * false otherwise.
   */
  def contains(k: K): Boolean = get(k).isDefined

  /* Returns this cache's key-value pairs as an immutable Map. */
  def toMap: Map[K, V] = iterator.toMap

  /* Returns a new cache containing the supplied key-value pair (and
   * possibly evicting some other key). */
  def +(kv: (K, V)): Cache[K, V] = put(kv)._2

  /* Returns a new Cache with this key evicted. */
  def -(k: K): Cache[K, V] = evict(k)._2

  /* Returns a new Cache with this key evicted. */
  def --(ks: Set[K]): Cache[K, V] = ks.foldLeft(this)(_ - _)

  /* Returns a new cache seeded with the kv-pairs in the supplied
   * map. */
  def seed(m: Map[K, V]): Cache[K, V] = m.foldLeft(empty)(_ + _)

  /**
   * Touches the cache with the supplied key. If the key is present
   * in the cache, the cache calls "hit" on the key. If the key is
   * missing, the cache adds fn(k) to itself.
   */
  def touch(k: K, v: => V): Cache[K, V] =
    if (contains(k))
      hit(k)
    else
      this + (k -> v)

  def toMutable(exhaustFn: Set[K] => Unit = _ => ()): MutableCache[K, V] = Cache.toMutable(this)(exhaustFn)
}
