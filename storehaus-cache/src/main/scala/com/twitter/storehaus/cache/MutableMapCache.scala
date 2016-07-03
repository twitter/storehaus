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

import scala.collection.mutable.{ Map => MutableMap }

object MutableMapCache {
  def apply[K, V](m: MutableMap[K, V]): MutableMapCache[K, V] = new MutableMapCache(m)
}
/**
  * MutableCache backed by a scala mutable Map.
  */

class MutableMapCache[K, V](m: MutableMap[K, V]) extends MutableCache[K, V] {
  override def get(k: K): Option[V] = m.get(k)
  override def +=(kv: (K, V)): this.type = { m += kv; this }
  override def hit(k: K): Option[V] = m.get(k)
  override def evict(k: K): Option[V] = {
    val ret = m.get(k)
    m -= k
    ret
  }
  override def empty: MutableMapCache[K, V] = new MutableMapCache(m.empty)
  override def clear: this.type = { m.clear; this }
  override def iterator: Iterator[(K, V)] = m.iterator
}
