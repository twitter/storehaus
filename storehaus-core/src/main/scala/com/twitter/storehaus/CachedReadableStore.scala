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

import com.twitter.storehaus.cache.MutableCache
import com.twitter.util.{ Future, Return }
import scala.collection.breakOut

class CachedReadableStore[K, V](
  store: ReadableStore[K, V],
  cache: MutableCache[K, Future[Option[V]]]
) extends ReadableStore[K, V] {
  val filteredCache = cache.filter {
    _._2.poll match {
      case None | Some(Return(_)) => true
      case _ => false
    }
  }

  /**
    * If a key is present and successful in the cache, use the cache
    * value. Otherwise (in a missing cache value or failed future),
    * refresh the cache from the store.
    */
  override def get(k: K): Future[Option[V]] = filteredCache.getOrElseUpdate(k, store.get(k))

  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] = {
    val present: Map[K1, Future[Option[V]]] =
      keys.map { k => k -> filteredCache.hit(k) }.collect { case (k, Some(v)) => k -> v }(breakOut)
    val replaced = store.multiGet(keys -- present.keySet)
    replaced.foreach { cache += _ }
    present ++ replaced
  }
}
