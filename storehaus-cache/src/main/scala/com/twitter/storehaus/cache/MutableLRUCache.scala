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
import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }

/**
  * Creates a mutable LRU cache based on an insertion-order
  * java.util.LinkedHashMap. As per the
  * [[com.twitter.storehaus.cache.MutableCache]] contract, gets will
  * NOT modify the underlying map.
  */

object MutableLRUCache {
  def apply[K, V](capacity: Int) = new MutableLRUCache[K, V](capacity)
}

class MutableLRUCache[K, V](capacity: Int) extends JMapCache[K, V](() =>
  new JLinkedHashMap[K, V](capacity + 1, 0.75f, true) {
    override protected def removeEldestEntry(eldest: JMap.Entry[K, V]) =
      super.size > capacity
  }) {
  override def hit(k: K) = {
    get(k).map { v =>
      evict(k)
      this += (k, v)
      v
    }
  }
}
