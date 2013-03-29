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

import com.twitter.storehaus.cache.{ Atomic, Cache, MutableCache }
import com.twitter.util.{ Future, Return, Throw }

class CachedReadableStore[K, V](store: ReadableStore[K, V], cache: MutableCache[K, Future[Option[V]]]) extends ReadableStore[K, V] {
  /**
    * If a key is present and successful in the cache, use the cache
    * value. Otherwise (in a missing cache value or failed future),
    * refresh the cache from the store.
    */
  override def get(k: K): Future[Option[V]] =
    cache.get(k) match {
      case Some(cached@Future(Return(_))) => {
        cache.hit(k)
        cached
      }
      case Some(Future(Throw(_))) | None => {
        val storeV = store.get(k)
        cache += (k -> storeV)
        storeV
      }
      case Some(cached) => {
        cache.hit(k)
        cached
      }
    }

  protected def needsRefresh(opt: Option[Future[_]]): Boolean =
    !opt.exists { f => f.isDefined && f.isThrow }

  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] = {
    type Pair = Set[(K1, Future[Option[V]])]
    val (pairsToReplace, pairsToHit): (Pair, Pair)  =
      keys.map { k => k -> cache.get(k) }.partition { case (_, f) => needsRefresh(f) }
    pairsToHit.foreach { case (k, _) => cache.hit(k) }
    val replaced = store.multiGet(pairsToReplace.map { _._1 })
    replaced.foreach { cache += _ }
    replaced ++ pairsToHit
  }
}
