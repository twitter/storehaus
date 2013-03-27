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

  override def put(kv: (K, V)): Cache[K, V] = {
    val (key, value) = kv
    val newIdx = idx + 1
    val (newMap, newOrd) =
      if (ord.size > maxSize) {
        val (idxToEvict, keyToEvict) =
          if (map.contains(key))
            (map(key)._1, key)
          else
            ord.min
        (map - keyToEvict, ord - idxToEvict)
      } else
        (map, ord)

    new LRUCache(maxSize, newIdx,
      newMap + (key -> (newIdx, value)),
      newOrd + (newIdx -> key))
  }

  override def evict(k: K): Cache[K, V] =
    map.get(k).map {
      case (oldIdx, _) =>
        new LRUCache(maxSize, idx + 1, map - k, ord - oldIdx)
    }.getOrElse(this)

  override def toString = {
    val pairStrings = map.map { case (k, (_, v)) => k + " -> " + v }
    "LRUCache(" + pairStrings.toList.mkString(", ") + ")"
  }
}
