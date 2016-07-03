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

import com.twitter.conversions.time._
import com.twitter.util._

/**
 * Use the ReadableStore abstraction when each read from the backend store involves
 * a time-taking task. A stream of backoffs are passed in so that we only wait for a
 * finite time period for the task to complete.
 */
class RetryingReadableStore[-K, +V](
  store: ReadableStore[K, V], backoffs: Iterable[Duration])(pred: Option[V] => Boolean)
  (implicit timer: Timer) extends ReadableStore[K, V] {

  private[this] def getWithRetry(k: K, backoffs: Iterable[Duration]): Future[Option[V]] =
    store.get(k).filter(pred) transform {
      case Return(t) => Future.value(t)
      case Throw(e) =>
        backoffs.headOption match {
          case None => FutureOps.retriesExhaustedFor(k)
          case Some(interval) => interval match {
            case Duration.Zero => getWithRetry(k, backoffs.tail)
            case Duration.Top => FutureOps.missingValueFor(k)
            case _ => Futures.flatten {
              timer.doLater(interval) {
                getWithRetry(k, backoffs.tail)
              }
            }
          }
        }
    }

  override def get(k: K): Future[Option[V]] = getWithRetry(k, backoffs)
}

/**
 * Delegate put to the underlying store and allow retriable semantics for get.
 */
class RetryingStore[-K, V](
  store: Store[K, V], backoffs: Iterable[Duration])(pred: Option[V] => Boolean)
  (implicit timer: Timer)
  extends RetryingReadableStore[K, V](store, backoffs)(pred) with Store[K, V] {

  private val paddedBackoffs = backoffs ++ Seq(0.second)

  private def find[T](futures: Iterator[(Future[T], Duration)])(pred: T => Boolean): Future[T] = {
    if (!futures.hasNext) {
      Future.exception(new RuntimeException("RetryingRWStore: empty iterator in function find"))
    } else {
      val (next, delay) = futures.next()
      if (!futures.hasNext) {
        next
      } else {
        next.filter(pred).rescue {
          case e: Exception =>
            timer.doLater(delay)(()) flatMap { _ =>
              find(futures)(pred)
            }
        }
      }
    }
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    store.multiPut(kvs) map { case (k, future) =>
      val retryStream = (Iterator(future) ++ Iterator.continually { store.put((k, kvs(k)))})
          .zip(paddedBackoffs.iterator)
      (k, find(retryStream) { t => true })
    }
  }

  override def put(kv: (K, Option[V])): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
}

