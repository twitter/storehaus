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

  implicit def enrichStore[StoreType <: Store[StoreType, K, V], K, V](store: StoreType): AlgebraicStore[StoreType, K, V] =
    new AlgebraicStore[StoreType, K, V](store)
}

class AlgebraicReadableStore[K, V](store: ReadableStore[K, V]) {
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

class AlgebraicStore[StoreType <: Store[StoreType, K, V], K, V](store: StoreType) {
  def aggregating(implicit sg: Monoid[V]): AggregatingStore[StoreType, K, V] = new AggregatingStore(store)
}

/**
 * Store which aggregates values added with + into the existing value in the store.
 * If addition ever results in a zero value, the key is deleted from the store.
 */
class AggregatingStore[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType)
extends Store[AggregatingStore[StoreType, K, V], K, V] {
  override def get(k: K) = store.get(k)
  override def multiGet(ks: Set[K]) = store.multiGet(ks)

  override def -(k: K): Future[AggregatingStore[StoreType, K, V]] =
    (store - k) map { new AggregatingStore(_) }

  override def +(pair: (K,V)): Future[AggregatingStore[StoreType, K, V]] = {
    val (k, v) = pair
    store.get(k) flatMap { oldV: Option[V] =>
      Monoid.nonZeroOption(oldV.map { Monoid.plus(_, v) } getOrElse v)
        .map { newV => store + (k -> newV) }
        .getOrElse(store - k)
        .map { new AggregatingStore(_) }
    }
  }

  override def update(k: K)(fn: Option[V] => Future[Option[V]]): Future[AggregatingStore[StoreType, K, V]] = {
    store.update(k)(fn) map { new AggregatingStore(_) }
  }
}
