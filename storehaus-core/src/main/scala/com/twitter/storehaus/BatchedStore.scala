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

import com.twitter.util.Future
import com.twitter.concurrent.AsyncSemaphore

/**
 * Adds batched writes to BatchedReadableStore
 *
 * @param store the store to read and write values to
 * @param maxMultiPutSize a multiPut to `store` will fetch values for at most `maxMultiPutSize` keys
 * @param maxConcurrentMultiPuts the maximum number of multiputs to concurrently issue
 * @param maxMultiGetSize a multiGet to `store` will fetch values for at most `maxMultiGetSize` keys
 * @param maxConcurrentMultiGets the maximum number of multigets to concurrently issue
 */
class BatchedStore[K, V](
    store: Store[K, V],
    maxMultiPutSize: Int,
    maxConcurrentMultiPuts: Int,
    maxMultiGetSize: Int,
    maxConcurrentMultiGets: Int)
    (implicit fc: FutureCollector)
      extends BatchedReadableStore[K, V](store, maxMultiGetSize, maxConcurrentMultiGets)
      with Store[K, V] {
  // do we need different knobs for gets and puts
  // or should we use the same max size and max concurrent for both?
  protected val writeConnectionLock = new AsyncSemaphore(maxConcurrentMultiPuts)

  override def put(kv: (K, Option[V])): Future[Unit] = store.put(kv)

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {

    kvs
      .grouped(maxMultiPutSize)
      .map{ keyBatch: Map[K1, Option[V]] =>

        // mapCollect the result of the multiput so we can release the permit at the end
        val batchResult: Future[Map[K1, Unit]] = writeConnectionLock
          .acquire()
          .flatMap { permit =>
            FutureOps.mapCollect(store.multiPut(keyBatch)).ensure{ permit.release() }
          }

        // now undo the mapCollect to yield a Map of future
        FutureOps.liftValues(keyBatch.keySet, batchResult)
      }
      .reduceOption(_ ++ _)
      .getOrElse(Map.empty)
  }
}
