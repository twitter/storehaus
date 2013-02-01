/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
b * a copy of the License at
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
 * The optional "capacity" parameter allows the store to buffer and aggregate
 * that many kv pairs in an internal SummingQueue before committing them all
 * out after "capacity" total kv pairs are submitted.
 *
 * @author Sam Ritchie
 */

object AggregatingStore {
  def apply[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType, capacity: Int = 0) =
    new AggregatingStore[StoreType, K, V](store, SummingQueue[Map[K, V]](capacity))
}

class AggregatingStore[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType,
                                                                          queue: SummingQueue[Map[K, V]])
extends Store[AggregatingStore[StoreType, K, V], K, V] {
  override def get(k: K) = submit(queue.flush).flatMap { _.get(k) }
  override def multiGet(ks: Set[K]) = submit(queue.flush).flatMap { _.multiGet(ks) }

  protected def update(s: StoreType, pair: (K, V)): Future[StoreType] = {
    val (k, v) = pair
    store.get(k) flatMap { oldV: Option[V] =>
      Monoid.nonZeroOption(oldV.map { Monoid.plus(_, v) } getOrElse v)
        .map { newV => store + (k -> newV) }
        .getOrElse(store - k)
    }
  }

  protected def submit(optM: Option[Map[K, V]]): Future[StoreType] = {
    val storeFuture = Future.value(store)
    optM.map { _.foldLeft(storeFuture) { (s, pair) => s.flatMap { update(_, pair) } } }
      .getOrElse(storeFuture)
  }

  override def -(k: K): Future[AggregatingStore[StoreType, K, V]] =
    submit(queue.flush)
      .flatMap { s => (s - k).map { new AggregatingStore(_, queue) } }

  override def +(pair: (K, V)): Future[AggregatingStore[StoreType, K, V]] =
    submit(queue(Map(pair)))
      .map { new AggregatingStore(_, queue) }

  override def update(k: K)(fn: Option[V] => Option[V]): Future[AggregatingStore[StoreType, K, V]] =
    submit(queue.flush)
      .flatMap { _.update(k)(fn).map { new AggregatingStore(_, queue) } }
}
