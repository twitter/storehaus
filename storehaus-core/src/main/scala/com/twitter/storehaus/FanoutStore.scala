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

import com.twitter.algebird.{Semigroup, Monoid}
import com.twitter.util.Future

/**
 * A store that fans out Key K to a Set of Keys, queries the appropriate underlying stores and sum
 * the values using the Monoid V
 *
 * @author Mansur Ashraf
 */
class FanoutStore[-K, +V: Semigroup, S <: ReadableStore[K, V]](fanout: K => Set[K], stores: Set[(K => Boolean, S)])
  extends ReadableStore[K, V] {

  override def get(k: K): Future[Option[V]] = {
    val s = implicitly[Semigroup[V]]
    val values = fanout(k)
      .groupBy {
        groupByStore
      }.flatMap {
        case (store, keys) => store.multiGet(keys).values
      }

    Future.collect(values.toSeq)
      .map { seq =>
        seq.reduceOption[Option[V]] {
          case (Some(x),Some(y))=> Some(s.plus(x,y))
          case (None,y@Some(_))=>  y
          case (x@Some(_),None)=>  x
          case _ => None
        }.getOrElse(None)
      }
  }

  private def groupByStore(k: K): S = {
    stores
      .find { case (fn, _) => fn(k) }
      .map { case (_, s) => s }
      .getOrElse(throw new IllegalStateException("no store found for key %s".format(k)))
  }
}

object FanoutStore {

  def apply[K, V: Monoid, S <: ReadableStore[K, V]](fanout: K => Set[K], stores: Set[(K => Boolean, S)]) = new FanoutStore[K, V, S](fanout, stores)
}

