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

import com.twitter.storehaus.Store

object StoreAlgebra {
  implicit def enrichStore[K, V](store: Store[K, V]): AlgebraicStore[K, V] =
    new AlgebraicStore[K, V](store)

  def unpivoted[K, OuterK, InnerK, V](store: Store[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK)): Store[K, V] =
    new UnpivotedStore(store)(split)
}

class AlgebraicStore[K, V](store: Store[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))(implicit ev: V <:< Map[InnerK, InnerV]): Store[CombinedK, InnerV] =
    StoreAlgebra.unpivoted(store.asInstanceOf[Store[K, Map[InnerK, InnerV]]])(split)
}
