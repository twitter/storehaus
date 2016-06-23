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
 * Basic cache implementation using an immutable backing map.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 */

object MapCache {
  def empty[K, V]: MapCache[K, V] = MapCache(Map.empty[K, V])
  def apply[K, V](m: Map[K, V]): MapCache[K, V] = new MapCache(m)
}

class MapCache[K, V](m: Map[K, V]) extends Cache[K, V] {
  override def get(k: K): Option[V] = m.get(k)
  override def contains(k: K): Boolean = m.contains(k)
  override def hit(k: K): MapCache[K, V] = this
  override def put(kv: (K, V)): (Set[K], MapCache[K, V]) = (Set.empty[K], new MapCache(m + kv))
  override def evict(k: K): (Option[V], MapCache[K, V]) =
    m.get(k).map { v => (Some(v), new MapCache(m - k)) }
      .getOrElse((None, this))
  override def toString: String = m.toString
  override def toMap: Map[K, V] = m
  override def iterator: Iterator[(K, V)] = m.iterator
  override def empty: MapCache[K, V] = new MapCache(Map.empty[K, V])
  override def seed(newPairs: Map[K, V]): MapCache[K, V] = new MapCache(newPairs)
}
