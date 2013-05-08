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

import com.twitter.bijection.Injection

/**
  * Enrichment on the Store[K, V] trait. Storehaus uses the enrichment
  * pattern instead of adding these methods directly to the trait
  * because many of the functions (mapValues, for example) have
  * different meanings for ReadableStore, Store and MergeableStore.
  *
  * {{{ import Store.enrich }}}
  *
  * to get access to these methods.
  *
  * TODO: in scala 2.10 this should be a value class
  */
class EnrichedStore[-K, V](store: Store[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))(implicit ev: V <:< Map[InnerK, InnerV]): Store[CombinedK, InnerV] =
    Store.unpivot(store.asInstanceOf[Store[K, Map[InnerK, InnerV]]])(split)

  def convert[K2, V2](kfn: K2 => K)(implicit inj: Injection[V2, V]): Store[K2, V2] =
    Store.convert[K, K2, V, V2](store)(kfn)(inj)

  def composeKeyMapping[K1](fn: K1 => K): Store[K1, V] =
    Store.convert(store)(fn)

  def mapValues[V1](implicit inj: Injection[V1, V]): Store[K, V1] =
    Store.convert(store)(identity[K])
}
