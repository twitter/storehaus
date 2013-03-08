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

import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection
import com.twitter.storehaus.{ FutureCollector, Store }
import com.twitter.util.Future

/**
  * Enrichments on Store.
  */
object StoreAlgebra {
  implicit def enrichStore[K, V](store: Store[K, V]): AlgebraicStore[K, V] =
    new AlgebraicStore[K, V](store)

  def unpivot[K, OuterK, InnerK, V](store: Store[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK)): Store[K, V] =
    new UnpivotedStore(store)(split)

  def convert[K1, K2, V1, V2](store: Store[K1, V1])(kfn: K2 => K1)(implicit inj: Injection[V2, V1]): Store[K2, V2] =
    new ConvertedStore(store)(kfn)
}

class AlgebraicStore[K, V](store: Store[K, V]) {
  def toMergeable(implicit mon: Monoid[V], fc: FutureCollector[(K, Option[V])]): MergeableStore[K, V] =
    MergeableStore.fromStore(store)

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))(implicit ev: V <:< Map[InnerK, InnerV]): Store[CombinedK, InnerV] =
    StoreAlgebra.unpivot(store.asInstanceOf[Store[K, Map[InnerK, InnerV]]])(split)

  def composeKeyMapping[K1](fn: K1 => K): Store[K1, V] = StoreAlgebra.convert(store)(fn)

  def mapValues[V1](implicit inj: Injection[V1, V]): Store[K, V1] = StoreAlgebra.convert(store)(identity[K])

  def convert[K1, V1](fn: K1 => K)(implicit inj: Injection[V1, V]): Store[K1, V1] =
    StoreAlgebra.convert(store)(fn)
}
