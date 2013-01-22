/*
 * Copyright 2010 Twitter Inc.
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

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object LRUStore {
  def apply[K,V](maxSize: Int = 1000) = new LRUStore[K,V](maxSize)
}
class LRUStore[K,V](maxSize: Int) extends JMapStore[LRUStore[K,V],K,V] {
  // create a java linked hashmap with access-ordering (LRU)
  protected lazy override val jstore = new JLinkedHashMap[K,Option[V]](maxSize + 1, 0.75f, true) {
    override protected def removeEldestEntry(eldest : JMap.Entry[K, Option[V]]) =
      super.size > maxSize
  }
}
