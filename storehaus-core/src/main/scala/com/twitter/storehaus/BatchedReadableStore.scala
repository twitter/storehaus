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

import com.twitter.util.{ Future }
import com.twitter.concurrent.AsyncSemaphore

/**
 * Ever wished you could do a multiGet for 10,000 keys, but spread out over several multiGets?
 * Use the BatchedReadableStore.
 *
 * @param store the store to fetch values from
 * @param maxMultiGetSize a multiGet to `store` will fetch values for at most `maxMultiGetSize` keys
 * @param maxConcurrentMultiGets the maximum number of multigets to concurrently issue
 */
class BatchedReadableStore[K, V](
    store: ReadableStore[K, V],
    maxMultiGetSize: Int,
    maxConcurrentMultiGets: Int)
    (implicit fc: FutureCollector[(K, V)]) extends ReadableStore[K, V] {
  override def get(k: K): Future[Option[V]] = store.get(k)
  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] = {

    val connectionLock = new AsyncSemaphore(maxConcurrentMultiGets)

    keys
      .grouped(maxMultiGetSize)
      .map{ keyBatch: Set[K1] =>

        // mapCollect the result of the multiget so we can release the permit at the end
        val batchResult: Future[Map[K1, Option[V]]] = connectionLock
          .acquire()
          .flatMap { permit =>
            FutureOps.mapCollect(store.multiGet(keyBatch)).ensure{ permit.release() }
          }

        // now undo the mapCollect to yield a Map of future
        FutureOps.liftValues(keyBatch, batchResult)
      }
      .reduceOption(_ ++ _)
      .getOrElse(Map.empty)
  }
}
