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
 * Store that buffers updates until the supplied capacity have been submitted.
 *
 * @author Sam Ritchie
 */

object BufferingStore {
  def apply[StoreType <: Store[StoreType, K, V], K, V: Monoid](store: StoreType, capacity: Int = 0) =
    new BufferingStore[StoreType, K, V](store, SummingQueue[Map[K, Option[V] => Option[V]]](capacity))
}

class BufferingStore[StoreType <: Store[StoreType, K, V], K, V: Monoid]
(store: StoreType, queue: SummingQueue[Map[K, Option[V] => Option[V]]])
extends Store[BufferingStore[StoreType, K, V], K, V] {

  protected def submit(optM: Option[Map[K, Option[V] => Option[V]]]): Future[StoreType] = {
    val storeFuture = Future.value(store)
    optM.map { _.foldLeft(storeFuture) { case (s, (k, fn)) => s.flatMap { _.update(k)(fn) } } }
      .getOrElse(storeFuture)
  }

  override def get(k: K) = submit(queue.flush).flatMap { _.get(k) }
  override def multiGet(ks: Set[K]) = submit(queue.flush).flatMap { _.multiGet(ks) }

  override def -(k: K): Future[BufferingStore[StoreType, K, V]] =
    submit(queue(Map(k -> { _ => None })))
      .map { new BufferingStore(_, queue) }

  override def +(pair: (K, V)): Future[BufferingStore[StoreType, K, V]] = {
    val (k, v) = pair
    submit(queue(Map(k -> { _ => Some(v) })))
      .map { new BufferingStore(_, queue) }
  }

  override def update(k: K)(fn: Option[V] => Option[V]): Future[BufferingStore[StoreType, K, V]] =
    submit(queue(Map(k -> fn)))
      .map { new BufferingStore(_, queue) }
}
