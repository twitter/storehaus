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

import scala.collection.breakOut
import com.twitter.util.Duration

/**
 * Immutable implementation of a *T*ime *T*o *L*ive cache.
 *
 * Every value placed into the cache via "put" must have an
 * accompanying expiration time. Alternatively, placing a (K, V) pair
 * into the TTLCache via putWithTime will generate an expiration time
 * via the supplied clock function. After the supplied time-to-live
 * Duration has passed, placing any new pair into the cache with
 * "put" will evict all expired keys.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

object TTLCache {
  def apply[K, V](ttl: Duration, backingMap: Map[K, (Long, V)] = Map.empty[K, (Long, V)]) =
    new TTLCache(ttl, backingMap)(() => System.currentTimeMillis)
}

class TTLCache[K, V](val ttl: Duration, cache: Map[K, (Long, V)])(val clock: () => Long) extends Cache[K, (Long, V)] {
  override def get(k: K) = cache.get(k)
  override def contains(k: K) = cache.contains(k)
  override def hit(k: K) = this

  override def put(kv: (K, (Long, V))) = putWithTime(kv, clock())

  override def evict(k: K): (Option[(Long, V)], Cache[K, (Long, V)]) =
    cache.get(k).map { pair: (Long, V) =>
      (Some(pair), new TTLCache(ttl, cache - k)(clock))
    }.getOrElse((None, this))

  override def empty = new TTLCache(ttl, cache.empty)(clock)
  override def iterator = cache.iterator
  override def toMap = cache.toMap

  protected def toRemove(currentMillis: Long): Set[K] =
    toMap.collect {
      case (k, (expiration, _)) if expiration < currentMillis => k
    }(breakOut)

  protected def putWithTime(kv: (K, (Long, V)), currentMillis: Long): (Set[K], TTLCache[K, V]) = {
    val killKeys = toRemove(currentMillis)
    val newCache = cache -- killKeys + kv
    (killKeys, new TTLCache(ttl, newCache)(clock))
  }

  /* Returns Some(v) if the value is present in the cache and
   * non-expired, None otherwise. */
  def getNonExpired(k: K): Option[V] =
    get(k).filter { case (expiration, _) => expiration > clock() }
      .map { _._2 }

  /* Returns a [[scala.util.collection.immutable.Map]] containing all
   * non-expired key-value pairs. */
  def toNonExpiredMap: Map[K, V] = {
    val now = clock()
    toMap.collect { case (k, (exp, v)) if exp > now => k -> v }
  }

  /* Returns true if the supplied key has expired, false otherwise. */
  def expired(k: K): Boolean = getNonExpired(k).isDefined

  /* Returns the set of entries from the cache with expiration dates
   * older than the value produced by the supplied clock at call-time
   * and a new cache with these entries evicted. */
  def removeExpired: (Set[K], TTLCache[K,V]) = {
    val killKeys = toRemove(clock())
    val newCache = cache -- killKeys
    (killKeys, new TTLCache(ttl, newCache)(clock))
  }

  /* Adds the supplied pair to the cache set to expire in (ttlInMillis +
   * the value produced by the supplied clock at call-time). */
  def putClocked(kv: (K, V)): (Set[K], TTLCache[K, V]) = {
    val (k, v) = kv
    val now = clock()
    putWithTime((k, (now + ttl.inMilliseconds, v)), now)
  }
}
