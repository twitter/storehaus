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

object MergeableStoreAlgebra {
  implicit def enrichMergeableStore[K, V](store: MergeableStore[K, V]): AlgebraicMergeableStore[K, V] =
    new AlgebraicMergeableStore[K, V](store)

  def unpivoted[K, OuterK, InnerK, V: Monoid](store: MergeableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): MergeableStore[K, V] =
    new UnpivotedMergeableStore(store)(split)

  def withSummer[K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]]): MergeableStore[K, V] =
    new BufferingStore(store, summer)
}

class AlgebraicMergeableStore[K, V](store: MergeableStore[K, V]) {
  def withSummer(summer: StatefulSummer[Map[K, V]]): MergeableStore[K, V] =
    MergeableStoreAlgebra.withSummer(store, summer)

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV], monoid: Monoid[InnerV])
      : MergeableStore[CombinedK, InnerV] =
    MergeableStoreAlgebra.unpivoted(store.asInstanceOf[MergeableStore[K, Map[InnerK, InnerV]]])(split)
}
