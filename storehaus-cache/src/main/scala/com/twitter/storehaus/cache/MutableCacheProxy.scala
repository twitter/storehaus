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

import com.twitter.util.{ Future, Time }

/** Proxy for Mergeables. Methods not overrided in extensions will be forwared to Proxied
 *  self member */
trait MutableCacheProxy[K, V] extends MutableCache[K, V] with CacheProxied[MutableCache[K,V]] {
  override def get(k: K): Option[V] = self.get(k)

  override def +=(kv: (K, V)): this.type = {
    self.+=(kv)
    this
  }

  override def multiInsert(kvs: Map[K, V]): this.type = {
    self.multiInsert(kvs)
    this
  }

  override def hit(k: K): Option[V] = self.hit(k)
  override def evict(k: K): Option[V] = self.evict(k)
  override def iterator: Iterator[(K, V)] = self.iterator
  override def empty: MutableCache[K, V] = self.empty

  override def clear: this.type = {
    self.clear
    this
  }

  override def contains(k: K): Boolean = self.contains(k)

  override def -=(k: K): this.type = {
    self.-=(k)
    this
  }

  override def multiRemove(ks: Set[K]): this.type = {
    self.multiRemove(ks)
    this
  }

  override def getOrElseUpdate(k: K, v: => V): V = self.getOrElseUpdate(k , v)
  override def filter(pred: ((K, V)) => Boolean): MutableCache[K, V] = self.filter(pred)
}