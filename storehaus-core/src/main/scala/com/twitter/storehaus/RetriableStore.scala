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

import com.twitter.util.{ Duration, Future, Return, Throw, Timer }

/**
 * Use the ReadableStore abstraction when each read from the backend store involves
 * a time-taking task. A stream of backoffs are passed in so that we only wait for a
 * finite time period for the task to complete.
 */
class RetriableReadableStore[K, V](store: ReadableStore[K, V], backoffs: Stream[Duration])(pred: Option[V] => Boolean)(implicit timer: Timer) extends ReadableStore[K, V] {

  private[this] def getWithRetry(k: K, backoffs: Stream[Duration]): Future[Option[V]] =
    store.get(k).filter(pred) transform {
      case Return(t) => Future.value(t)
      case Throw(e) =>
        if (backoffs.isEmpty) {
          FutureOps.missingValueFor(k)
        } else {
          Future.flatten {
            timer.doLater(backoffs.head) {
              getWithRetry(k, backoffs.tail)
            }
          }
        }
    }

  override def get(k: K) = getWithRetry(k, backoffs)
}

/**
 * Delegate put to the underlying store and allow retriable semantics for get.
 */
class RetriableStore[K, V](store: Store[K, V], backoffs: Stream[Duration])(pred: Option[V] => Boolean)(implicit timer: Timer)
  extends RetriableReadableStore[K, V](store, backoffs)(pred)
  with Store[K, V] {
  override def put(kv: (K, Option[V])) = store.put(kv)
}
