/*
 * Copyright 2014 Twitter Inc.
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
 * Provides read-through caching on a readable store fronted by a cache.
 *
 * @author Ruban Monu
 */
class ReadThroughStore[K, V](backingStore: ReadableStore[K, V], cache: Store[K, V])
  extends ReadableStore[K, V] {

  override def get(k: K): Future[Option[V]] = {
    cache.get(k).flatMap { cacheValue =>
      cacheValue match {
        case None => backingStore.get(k).flatMap { storeValue =>
          cache.put((k, storeValue)).map { u : Unit => storeValue }
        }
        case _ => Future.value(cacheValue)
      }
    }
  }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val result: Future[Map[K1, Option[V]]] =
      FutureOps.mapCollect(cache.multiGet(ks)).flatMap { cacheResult =>
        val hits = cacheResult.filter { !_._2.isEmpty }
        val missedKeys = cacheResult.filter { _._2.isEmpty }.keySet
        FutureOps.mapCollect(backingStore.multiGet(missedKeys)).flatMap { storeResult =>
          FutureOps.mapCollect(cache.multiPut(storeResult)).map { u =>
            hits ++ storeResult
          }
          // mapCollect will fail the future if any keys fail to write to cache.
          // TODO: if cache write fails for some keys in the batch,
          // should we invalidate other keys in this batch?
          // or should we make the cache writes best effort?
        }
      }
    FutureOps.liftValues(ks, result, { (k: K1) => Future.None })
  }
}

