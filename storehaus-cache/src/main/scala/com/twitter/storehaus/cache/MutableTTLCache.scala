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

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }
import com.twitter.util.Duration

object MutableTTLCache {
  def apply[K, V](ttl: Duration, capacity: Int) = {
    val backingCache = JMapCache[K, (Long, V)](
      new JLinkedHashMap[K, (Long, V)](capacity + 1, 0.75f) {
        override protected def removeEldestEntry(eldest: JMap.Entry[K, (Long, V)]) =
          super.size > capacity
      })
    new MutableTTLCache(ttl, backingCache)(() => System.currentTimeMillis)
  }
}

class MutableTTLCache[K, V](val ttl: Duration, protected val backingCache: MutableCache[K, (Long, V)])(val clock: () => Long) extends MutableCache[K, V] {
  private[this] val putsSincePrune = new java.util.concurrent.atomic.AtomicInteger(1)

  def get(k: K) = {
    val clockVal = clock()
    backingCache.get(k).filter(_._1 > clockVal).map(_._2)
  }

  def +=(kv: (K, V)): this.type = {
    if(putsSincePrune.getAndIncrement % 1000 == 0) {
      removeExpired
      putsSincePrune.set(1)
    }
    backingCache += (kv._1, (clock() + ttl.inMilliseconds, kv._2))
    this
  }

  def hit(k: K) = {
    backingCache.get(k).map{case (ts, v) =>
      this += ((k, v))
      v
    }
  }

  /* Returns an option of the (potentially) evicted value. */
  def evict(k: K) = {
    backingCache.evict(k).map(_._2)
  }

  def clear = {
    backingCache.clear
    this
  }

  override def contains(k: K) = get(k).isDefined
  override def empty = new MutableTTLCache(ttl, backingCache.empty)(clock)
  override def iterator = {
    val iteratorStartTime = clock()
    backingCache.iterator.flatMap{case (k, (ts, v)) =>
      if(ts >= iteratorStartTime)
        Some((k, v))
      else
        None
    }.toList.iterator
  }

  /* Returns a [[scala.util.collection.immutable.Map]] containing all
   * non-expired key-value pairs. */
  def toNonExpiredMap: Map[K, V] = iterator.toMap

  protected def toRemove: Set[K] = {
    val pruneTime = clock()
    backingCache.iterator.filter(_._2._1 < pruneTime).map(_._1).toSet
  }

  def removeExpired = {
    val killKeys = toRemove
    killKeys.foreach(backingCache.evict(_))
  }
}
