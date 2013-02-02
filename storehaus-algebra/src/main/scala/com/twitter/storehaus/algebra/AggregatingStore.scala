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

import com.twitter.algebird.{ Monoid, SummingQueue }
import com.twitter.util.Future
import com.twitter.storehaus.Store

/**
 * Store which aggregates values added with + into the existing value in the store.
 * If addition ever results in a zero value, the key is deleted from the store.
 *
 * @author Sam Ritchie
 */

object AggregatingStore {
  def apply[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType) =
    new AggregatingStore[StoreType, K, V](store)
}

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
  override def update(k: K)(fn: Option[V] => Option[V]): Future[AggregatingStore[StoreType, K, V]] =
    store.update(k)(fn) map { new AggregatingStore(_) }
}
