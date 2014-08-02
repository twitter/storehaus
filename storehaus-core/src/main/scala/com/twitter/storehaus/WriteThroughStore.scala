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
 * Provides write-through caching on a store fronted by a cache.
 *
 * All cache operations are best effort. i.e. a write will succeed if the key
 * was written to backing store even when adding/updating the cached copy of the key fails.
 *
 * On the other hand, any failure while writing to backing store
 * is propagated to the client.
 *
 * If invalidate flag is set, any keys that fail to write to backing store
 * are attempted to be removed from cache, rather than having the old value
 * still in cache.
 *
 * Thread-safety is achieved using a mutex.
 *
 * @author Ruban Monu
 */
class WriteThroughStore[K, V](backingStore: Store[K, V], cache: Store[K, V], invalidate: Boolean = true)
  extends ReadThroughStore[K, V](backingStore, cache) with Store[K, V] {

  override def put(kv: (K, Option[V])): Future[Unit] = mutex.acquire.flatMap { p =>
    // write key to backing store first
    backingStore.put(kv).flatMap { _ =>
      // now write key to cache, best effort
      cache.put(kv) rescue { case _ => Future.Unit }
    } rescue { case x =>
      // write to backing store failed
      // now optionally invalidate the key in cache, best effort
      if (invalidate) {
        cache.put((kv._1, None)) transform { _ => Future.exception(x) }
      } else {
        Future.exception(x)
      }
    } ensure {
      p.release
    }
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    val f : Future[Map[K1, Either[Unit, Exception]]] = mutex.acquire.flatMap { p =>
      // write keys to backing store first
      val storeResults : Map[K1, Future[Either[Unit, Exception]]] =
        backingStore.multiPut(kvs).map { case (k, f) =>
          (k, f.map { u: Unit => Left(u) }.rescue { case x: Exception => Future.value(Right(x)) })
        }

      // perform cache operations based on how writes to backing store go
      FutureOps.mapCollect(storeResults).flatMap { storeResult : Map[K1, Either[Unit, Exception]] =>
        val failedKeys = storeResult.filter { _._2.isRight }.keySet
        val succeededKeys = storeResult.filter { _._2.isLeft }.keySet

        // write updated keys to cache, best effort
        val keysToUpdate = kvs.filterKeys { succeededKeys.contains(_) } ++ {
            // optionally invalidate cached copy when write to backing store fails
            if (invalidate) { failedKeys.map { k => (k, None) }.toMap }
            else { Map.empty }
          }

        FutureOps.mapCollect(cache.multiPut(keysToUpdate))(FutureCollector.bestEffort[(K1, Unit)])
          .map { f => storeResult }
        // return original writes made to backing store
        // once cache operations are complete
      } ensure {
        p.release
      }
    }

    // throw original exception for any writes that failed
    FutureOps.liftValues(kvs.keySet, f, { (k: K1) => Future.None })
      .map { case kv : (K1, Future[Either[Unit, Exception]]) =>
        val transform = kv._2.map { v =>
          v match {
            case Left(optv) => optv
            case Right(x) => throw x
          }
        }
        (kv._1, transform)
      }
  }
}

