
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
import com.twitter.storehaus.{ AbstractReadableStore, ReadableStore, Store }

/**
 * import StoreAlgebra.enrich to obtain enrichments.
 */
object StoreAlgebra {
  implicit def enrichReadableStore[K, V](store: ReadableStore[K, V]): AlgebraicReadableStore[K, V] =
    new AlgebraicReadableStore[K, V](store)

  implicit def enrichStore[K, V](store: Store[K, V]): AlgebraicStore[K, V] =
    new AlgebraicStore[K, V](store)

  implicit def enrichMergeableStore[K, V](store: MergeableStore[K, V]): AlgebraicMergeableStore[K, V] =
    new AlgebraicMergeableStore[K, V](store)
}

class AlgebraicReadableStore[K, V](store: ReadableStore[K, V]) {
  /**
   * If V is TraversableOnce[T], returns a new store that sums V down into a single T
   * before returning. We require a Monoid to distinguish empty keys from keys with empty
   * Traversable values.
   */
  def summed[T](implicit ev: V <:< TraversableOnce[T], mon: Monoid[T]): ReadableStore[K, T] =
    new AbstractReadableStore[K, T] {
      override def get(k: K) = store.get(k) map { _ map { mon.sum(_) } }
      override def multiGet[K1<:K](ks: Set[K1]) =
        store.multiGet(ks).mapValues { fv : Future[Option[V]] =>
          fv.map { optV => optV.map { ts => mon.sum(ev(ts)) } }
        }
    }

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV]): ReadableStore[CombinedK, InnerV] =
    new UnpivotedReadableStore[CombinedK, K, InnerK, InnerV](
      store.asInstanceOf[ReadableStore[K, Map[InnerK, InnerV]]]
    )(split)
}


class AlgebraicStore[K, V](store: Store[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV]): Store[CombinedK, InnerV] =
    new UnpivotedStore[CombinedK, K, InnerK, InnerV](
      store.asInstanceOf[Store[K, Map[InnerK, InnerV]]]
    )(split)
}

class AlgebraicMergeableStore[K, V](store: MergeableStore[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV], monoid: Monoid[InnerV]): MergeableStore[CombinedK, InnerV] =
    new UnpivotedMergeableStore[CombinedK, K, InnerK, InnerV](
      store.asInstanceOf[MergeableStore[K, Map[InnerK, InnerV]]]
    )(split)
}
