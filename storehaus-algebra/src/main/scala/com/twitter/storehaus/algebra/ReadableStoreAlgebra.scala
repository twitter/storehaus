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
import com.twitter.util.Future
import com.twitter.storehaus.{ AbstractReadableStore, ReadableStore }

/**
  * Enrichments on ReadableStore.
  */

object ReadableStoreAlgebra {
  implicit def enrichReadableStore[K, V](store: ReadableStore[K, V]): AlgebraicReadableStore[K, V] =
    new AlgebraicReadableStore[K, V](store)

  /**
   * If V is TraversableOnce[T], returns a new store that sums V down into a single T
   * before returning. We require a Monoid to distinguish empty keys from keys with empty
   * Traversable values.
   */
  def summed[K, V, T](store: ReadableStore[K, V])(implicit ev: V <:< TraversableOnce[T], mon: Monoid[T]): ReadableStore[K, T] =
    new AbstractReadableStore[K, T] {
      override def get(k: K) = store.get(k) map { _ map { mon.sum(_) } }
      override def multiGet[K1 <: K](ks: Set[K1]) =
        store.multiGet(ks).mapValues { fv: Future[Option[V]] =>
          fv.map { optV => optV.map { ts => mon.sum(ev(ts)) } }
        }
    }

  def unpivoted[K, OuterK, InnerK, V](store: ReadableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): ReadableStore[K, V] =
    new UnpivotedReadableStore(store)(split)
}

class AlgebraicReadableStore[K, V](store: ReadableStore[K, V]) {
  def summed[T](implicit ev: V <:< TraversableOnce[T], mon: Monoid[T]): ReadableStore[K, T] =
    ReadableStoreAlgebra.summed(store)

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV])
      : ReadableStore[CombinedK, InnerV] =
    ReadableStoreAlgebra.unpivoted(store.asInstanceOf[ReadableStore[K, Map[InnerK, InnerV]]])(split)

  /**
   * convert K1 to K then lookup K in this store
   */
  def composeKeyMapping[K1](fn: K1 => K): ReadableStore[K1, V] =
    ReadableStore.convert(store)(fn) { (v: V) => Future.value(v) }

  /**
   * apply an async function on all the values
   */
  def flatMapValues[V2](fn: V => Future[V2]): ReadableStore[K, V2] =
    ReadableStore.convert(store)(identity[K])(fn)

  /**
   * Apply a non-blocking function on all the values. If this function throws, it will be caught in
   * the Future
   */
  def mapValues[V1](fn: V => V1): ReadableStore[K, V1] =
    flatMapValues(fn.andThen { Future.value(_) })

  def convert[K1, V1](kfn: K1 => K)(vfn: V => Future[V1]): ReadableStore[K1, V1] =
    ReadableStore.convert(store)(kfn)(vfn)
}
