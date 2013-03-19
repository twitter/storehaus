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

import com.twitter.algebird.{ Monoid, StatefulSummer }
import com.twitter.bijection.ImplicitBijection

class EnrichedMergeableStore[K, V](store: MergeableStore[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV], monoid: Monoid[InnerV])
      : MergeableStore[CombinedK, InnerV] =
    MergeableStore.unpivot(store.asInstanceOf[MergeableStore[K, Map[InnerK, InnerV]]])(split)

  def withSummer(summer: StatefulSummer[Map[K, V]]): MergeableStore[K, V] =
    MergeableStore.withSummer(store, summer)

  def composeKeyMapping[K1](fn: K1 => K): MergeableStore[K1, V] =
    MergeableStore.convert(store)(fn)

  def mapValues[V1](implicit bij: ImplicitBijection[V1, V]): MergeableStore[K, V1] =
    MergeableStore.convert(store)(identity[K])

  def convert[K1, V1](fn: K1 => K)(implicit bij: ImplicitBijection[V1, V]): MergeableStore[K1, V1] =
    MergeableStore.convert(store)(fn)
}
