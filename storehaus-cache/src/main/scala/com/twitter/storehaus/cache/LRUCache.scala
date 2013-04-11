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

import scala.collection.SortedMap

/**
 * Immutable implementation of an LRU cache.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

/**
 * "map" is the backing store used to hold key -> (index,value)
 * pairs. The index tracks the access time for a particular key. "ord"
 * is used to determine the Least-Recently-Used key in "map" by taking
 * the minimum index.
 */

object LRUCache {
  def apply[K, V](maxSize: Long, backingMap: Map[K, (Long, V)] = Map.empty[K, (Long, V)]) =
    new LRUCache(maxSize, 0, backingMap, SortedMap.empty[Long, K])
}

class LRUCache[K, V](maxSize: Long, idx: Long, map: Map[K, (Long, V)], ord: SortedMap[Long, K]) extends Cache[K, V] {
  // Scala's SortedMap requires an ordering on pairs. To guarantee
  // sorting on index only, LRUCache defines an implicit ordering on K
  // that treats all K as equal.
  protected implicit val kOrd = new Ordering[K] { def compare(l: K, r: K) = 0 }

  override def get(k: K): Option[V] = map.get(k).map { _._2 }

  override def contains(k: K): Boolean = map.contains(k)

  override def hit(k: K): Cache[K, V] =
    map.get(k).map {
      case (oldIdx, v) =>
        val newIdx = idx + 1
        val newMap = map + (k -> (newIdx, v))
        val newOrd = ord - oldIdx + (newIdx -> k)
        new LRUCache(maxSize, newIdx, newMap, newOrd)
    }.getOrElse(this)

  override def put(kv: (K, V)) = {
    val (key, value) = kv
    val newIdx = idx + 1
    val (evictedKeys, newMap, newOrd) =
      if (ord.size >= maxSize) {
        val (idxToEvict, keyToEvict) =
          map.get(key).map { case (idx, _) => (idx, key) }
            .getOrElse(ord.min)
        (Set(keyToEvict), map - keyToEvict, ord - idxToEvict)
      } else
        (Set.empty[K], map, ord)

    (evictedKeys, new LRUCache(maxSize, newIdx,
      newMap + (key -> (newIdx, value)),
      newOrd + (newIdx -> key)))
  }

  override def evict(k: K): (Option[V], Cache[K, V]) =
    map.get(k).map {
      case (oldIdx, v) =>
        (Some(v), new LRUCache(maxSize, idx + 1, map - k, ord - oldIdx))
    }.getOrElse((None, this))

  override def toString = {
    val pairStrings = iterator.map { case (k, v) => k + " -> " + v }
    "LRUCache(" + pairStrings.toList.mkString(", ") + ")"
  }

  override def empty = new LRUCache(maxSize, 0, map.empty, ord.empty)
  override def iterator = map.iterator.map { case (k, (_, v)) => k -> v }
  override def toMap = map.mapValues { _._2 }
}
