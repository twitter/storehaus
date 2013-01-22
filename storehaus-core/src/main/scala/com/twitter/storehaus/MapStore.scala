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

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// MapStore is an immutable store.
class MapStore[K,V](val backingStore: Map[K,V] = Map[K,V]()) extends KeysetStore[MapStore[K,V],K,V] {
  override def keySet = backingStore.keySet
  override def size = backingStore.size
  override def get(k: K) = Future.value(backingStore.get(k))
  override def multiGet(ks: Set[K]) = Future.value(Store.zipWith(ks) { backingStore.get(_) })
  override def -(k: K) = Future.value(new MapStore(backingStore - k))
  override def +(pair: (K, V)) = Future.value(new MapStore(backingStore + pair))
}
