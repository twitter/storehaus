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

import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap }

/**
  * LRUCache
  */

class LRUCache[K, V](maxSize: Int) extends JMapCache[K, V](
  new JLinkedHashMap[K, V](maxSize + 1, 0.75f, false) {
    override protected def removeEldestEntry(eldest: JMap.Entry[K, V]) =
      super.size > maxSize
  }) {
  // Put the current value back into the LinkedHashMap to refresh the
  // key.
  override def hit(k: K) = {
    get(k).foreach { m.put(k, _) }
    this
  }
}
