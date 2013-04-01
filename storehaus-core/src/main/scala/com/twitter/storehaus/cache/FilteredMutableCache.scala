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

class FilteredMutableCache[K, V](cache: MutableCache[K, V])(fn: V => Boolean) extends MutableCache[K, V] {
  override def get(k: K) = cache.get(k) match {
    case opt@Some(v) => if (fn(v)) opt else { cache.evict(k); None }
    case None => None
  }
  override def contains(k: K) = get(k).isDefined
  override def +=(kv: (K, V)) = {
    if (fn(kv._2)) cache += kv
    this
  }
  override def hit(k: K) = cache.hit(k)
  override def evict(k: K) = cache.evict(k)
  override def empty = new FilteredMutableCache(cache.empty)(fn)
  override def clear = { cache.clear; this }
  override def iterator = cache.iterator
}
