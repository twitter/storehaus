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

import com.twitter.util.Future

/** MapStore is a ReadableStore backed by a scala immutable Map.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */
class MapStore[K, +V](val backingStore: Map[K, V] = Map[K, V]()) extends ReadableStore[K, V]
    with IterableStore[K, V] {
  override def get(k: K) = Future.value(backingStore.get(k))

  override def getAll = IterableStore.iteratorToSpool(backingStore.iterator)
}
