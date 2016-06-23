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
  * MutableCache that applies a filter to each value.
  */

class FilteredMutableCache[K, V](cache: MutableCache[K, V])(pred: ((K, V)) => Boolean)
    extends MutableCache[K, V] {
  override def get(k: K): Option[V] = cache.get(k).filter(v => pred((k, v)))
  override def contains(k: K): Boolean = get(k).isDefined
  override def +=(kv: (K, V)): this.type = {
    if (pred(kv)) cache += kv
    this
  }
  override def hit(k: K): Option[V] = cache.hit(k) match {
    case opt@Some(v) => if (pred((k, v))) opt else { cache.evict(k); None }
    case None => None
  }
  override def evict(k: K): Option[V] = cache.evict(k)
  override def empty: FilteredMutableCache[K, V] = new FilteredMutableCache(cache.empty)(pred)
  override def clear: this.type = { cache.clear; this }
  override def iterator: Iterator[(K, V)] = cache.iterator.filter(pred)
}
