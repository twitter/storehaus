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

import java.util.{ Map => JMap }
import scala.collection.JavaConverters._

object JMapCache {
  def apply[K, V](supplier: => JMap[K, V]): JMapCache[K, V] = new JMapCache(() => supplier)
}

/**
  * Returns a MutableCache instance backed by the supplied
  * java.util.Map.
  */
class JMapCache[K, V](supplier: () => JMap[K, V]) extends MutableCache[K, V] {
  val m = supplier()
  m.clear()

  override def get(k: K): Option[V] = Option(m.get(k))
  override def contains(k: K): Boolean = m.containsKey(k)
  override def +=(kv: (K, V)): this.type = {
    m.put(kv._1, kv._2)
    this
  }
  override def hit(k: K): Option[V] = Option(m.get(k))
  override def evict(k: K): Option[V] = Option(m.remove(k))
  override def clear: this.type = { m.clear(); this }
  override def empty: JMapCache[K, V] = new JMapCache(supplier)
  override def iterator: Iterator[(K, V)] = m.entrySet.iterator.asScala.map { entry =>
    (entry.getKey, entry.getValue)
  }
}
