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
  def empty[K, V] = MapCache(Map.empty[K, V])
  def apply[K, V](m: Map[K, V]) = new MapCache(m)
}

class MapCache[K, V](m: Map[K, V]) extends Cache[K, V] {
  override def get(k: K) = m.get(k)
  override def contains(k: K) = m.contains(k)
  override def hit(k: K) = this
  override def put(kv: (K, V)) = (Set.empty[K], new MapCache(m + kv))
  override def evict(k: K) =
    m.get(k).map { v => (Some(v), new MapCache(m - k)) }
      .getOrElse((None, this))
  override def toString = m.toString
  override def toMap = m
  override def iterator = m.iterator
  override def empty = new MapCache(Map.empty)
  override def seed(newPairs: Map[K, V]) = new MapCache(newPairs)
}
