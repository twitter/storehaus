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

package com.twitter.storehaus.algebra

import com.twitter.algebird.{ Monoid, Semigroup }
import com.twitter.util.Future
import com.twitter.storehaus.{ ReadableStore, Store }

/**
 * import StoreAlgebra.enrich to obtain enrichments.
 */
object StoreAlgebra {
  implicit def enrichReadableStore[K, V](store: ReadableStore[K, V]): AlgebraicReadableStore[K, V] =
    new AlgebraicReadableStore[K, V](store)

  implicit def enrichStore[StoreType <: Store[StoreType, K, V], K, V: Monoid]
  (store: StoreType): AlgebraicStore[StoreType, K, V] =
    new AlgebraicStore[StoreType, K, V](store)
}

class AlgebraicReadableStore[K, V](store: ReadableStore[K, V]) {
  import ReadableStoreMonoid.apply

  /**
   * Returns a new store that queries this store and the supplied other store
   * and returns an option of both values summed together.
   */
  def +(other: ReadableStore[K, V])(implicit sg: Semigroup[V]): ReadableStore[K, V] =
    Monoid.plus(store, other)

  /**
   * If V is TraversableOnce[T], returns a new store that sums V down into a single T
   * before returning.
   */
  def summed[T](implicit ev: V <:< TraversableOnce[T], sg: Semigroup[T]): ReadableStore[K, T] =
    new ReadableStore[K, T] {
      override def get(k: K) = store.get(k) map { _ flatMap { Semigroup.sumOption(_) } }
      override def multiGet(ks: Set[K]) =
        store.multiGet(ks) map { kv =>
          kv.mapValues { fs: Future[Option[V]] =>
            fs.map { opt => opt.flatMap { ts => Semigroup.sumOption(ev(ts)) } }
          }
        }
    }
}

class AlgebraicStore[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType) {
  def buffering(capacity: Int = 0): BufferingStore[StoreType, K, V] = BufferingStore(store)
  lazy val aggregating: AggregatingStore[StoreType, K, V] = AggregatingStore(store)
}
