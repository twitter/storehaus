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

/**
  * Enrichments on MergeableStore.
  */

object MergeableStoreAlgebra {
  implicit def enrichMergeableStore[K, V](store: MergeableStore[K, V]): AlgebraicMergeableStore[K, V] =
    new AlgebraicMergeableStore[K, V](store)

  def unpivoted[K, OuterK, InnerK, V: Monoid](store: MergeableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): MergeableStore[K, V] =
    new UnpivotedMergeableStore(store)(split)

  def withSummer[K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]]): MergeableStore[K, V] =
    new BufferingStore(store, summer)

  def convert[K1, K2, V1, V2](store: MergeableStore[K1, V1])(kfn: K2 => K1)
    (implicit bij: ImplicitBijection[V2, V1]): MergeableStore[K2, V2] =
    new ConvertedMergeableStore(store)(kfn)
}

class AlgebraicMergeableStore[K, V](store: MergeableStore[K, V]) {
  def withSummer(summer: StatefulSummer[Map[K, V]]): MergeableStore[K, V] =
    MergeableStoreAlgebra.withSummer(store, summer)

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV], monoid: Monoid[InnerV])
      : MergeableStore[CombinedK, InnerV] =
    MergeableStoreAlgebra.unpivoted(store.asInstanceOf[MergeableStore[K, Map[InnerK, InnerV]]])(split)

  def composeKeyMapping[K1](fn: K1 => K): MergeableStore[K1, V] =
    MergeableStoreAlgebra.convert(store)(fn)

  def mapValues[V1](implicit bij: ImplicitBijection[V1, V]): MergeableStore[K, V1] =
    MergeableStoreAlgebra.convert(store)(identity[K])

  def convert[K1, V1](fn: K1 => K)(implicit bij: ImplicitBijection[V1, V]): MergeableStore[K1, V1] =
    MergeableStoreAlgebra.convert(store)(fn)
}
