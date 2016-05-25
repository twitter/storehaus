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
import com.twitter.storehaus.cache.{ Cache, MutableCache }

/**
  * Enrichment on the [[com.twitter.storehaus.ReadableStore]]
  * trait. Storehaus uses the enrichment pattern instead of adding
  * these methods directly to the trait because many of the functions
  * (mapValues, for example) have different meanings for
  * ReadableStore, Store and MergeableStore.
  *
  * {{{ import ReadableStore.enrich }}}
  *
  * to get access to these methods.
  *
  * TODO: in scala 2.10 this should be a value class
  */
class EnrichedReadableStore[-K, +V](store: ReadableStore[K, V]) {
  def andThen[V2, K1 >: V](other: ReadableStore[K1, V2])(implicit fc: FutureCollector): ReadableStore[K, V2] =
    new ComposedStore[K, V, V2, K1](store, other)

  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV])
      : ReadableStore[CombinedK, InnerV] =
    ReadableStore.unpivot(store.asInstanceOf[ReadableStore[K, Map[InnerK, InnerV]]])(split)

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
   * Apply a non-blocking function on all the values. If this function
   * throws, it will be caught in the Future.
   */
  def mapValues[V1](fn: V => V1): ReadableStore[K, V1] =
    flatMapValues(fn.andThen { Future.value(_) })

  def convert[K1, V1](kfn: K1 => K)(vfn: V => Future[V1]): ReadableStore[K1, V1] =
    ReadableStore.convert(store)(kfn)(vfn)

  /* Returns a new ReadableStore that caches reads from the underlying
   * store using the supplied mutable cache.  */
  def withCache[K1 <: K, V1 >: V](cache: MutableCache[K1, Future[Option[V1]]]): ReadableStore[K1, V1] =
    new CachedReadableStore(store, cache)

  /* Returns a new ReadableStore that caches reads from the underlying
   * store using the supplied immutable cache.  */
  def withCache[K1 <: K, V1 >: V](cache: Cache[K1, Future[Option[V1]]]): ReadableStore[K1, V1] =
    new CachedReadableStore(store, cache.toMutable())
}
