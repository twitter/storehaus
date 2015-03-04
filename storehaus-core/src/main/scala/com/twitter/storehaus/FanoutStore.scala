/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.twitter.storehaus

import com.twitter.algebird.Semigroup
import com.twitter.util.Future

/**
 * A store that fans out Key K to a Set of Keys, queries the appropriate underlying stores and sum
 * the values using the semigroup V
 *
 * @author Mansur Ashraf
 */
class FanoutStore[-K, K1, +V: Semigroup, S <: ReadableStore[K1, V]](fanout: K => Iterable[K1])(storeLookupFn: K1 => S)
  extends ReadableStore[K, V] {

  override def get(k: K): Future[Option[V]] = {
    val s = implicitly[Semigroup[V]]
    val values = fanout(k)
      .groupBy {
        storeLookupFn
      }.flatMap {
        case (store, keys) => store.multiGet(keys.toSet).values
      }

    Future.collect(values.toSeq)
      .map { seq =>
        seq.reduceOption[Option[V]] {
          case (Some(x), Some(y)) => Some(s.plus(x, y))
          case (None, y @ Some(_)) => y
          case (x @ Some(_), None) => x
          case _ => None
        }.getOrElse(None)
      }
  }
}

object FanoutStore {

  def apply[K, K1, V: Semigroup, S <: ReadableStore[K1, V]](fanout: K => Iterable[K1])(storeLookupFn: K1 => S) = new FanoutStore[K, K1, V, S](fanout)(storeLookupFn)
}

