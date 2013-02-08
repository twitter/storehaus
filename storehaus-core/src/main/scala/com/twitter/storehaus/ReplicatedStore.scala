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

import com.twitter.util.Future

import Store.{selectFirstSuccessfulTrial => selectFirst}

/**
 * Replicates writes to all stores, and takes the first successful read.
 */
class ReplicatedStore[StoreType <: Store[StoreType, K, V], K, V](stores: Seq[StoreType])
    extends Store[ReplicatedStore[StoreType, K, V], K, V] {
  override def get(k: K) = selectFirst(stores.map { _.get(k) })
  override def multiGet(ks: Set[K]) = selectFirst(stores.map { _.multiGet(ks) })
  override def update(k: K)(fn: Option[V] => Option[V]) =
    Future.collect(stores.map { _.update(k)(fn) }).map { new ReplicatedStore(_) }
  override def -(k: K) =
    Future.collect(stores.map { _ - k }).map { new ReplicatedStore(_) }
  override def +(pair: (K,V)) =
    Future.collect(stores.map { _ + pair }).map { new ReplicatedStore(_) }
}