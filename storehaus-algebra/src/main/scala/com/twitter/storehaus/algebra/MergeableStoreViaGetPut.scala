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

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{ FutureCollector, Store }
import com.twitter.util.Future

/**
 * Just get, locally merge, then put. This is only safe if there is only one
 * writer thread per key. Otherwise you need to do some locking or compare-and-swap
 * in the store
 */

class MergeableStoreViaSingleGetPut[-K, V: Semigroup](store: Store[K, V]) extends MergeableStore[K, V] {
  override def semigroup: Semigroup[V] = implicitly[Semigroup[V]]

  override def get(k: K) = store.get(k)
  override def put(kv: (K, Option[V])) = store.put(kv)

  /**
   * sets to .plus(get(kv._1).get.getOrElse(monoid.zero), kv._2)
   * but maybe more efficient implementations
   */
  override def merge(kv: (K, V)) =
    for {
      vOpt <- get(kv._1)
      newVOpt = vOpt.map(Semigroup.plus(_, kv._2)).orElse(Some(kv._2))
      finalUnit <- put((kv._1, newVOpt))
    } yield vOpt
}

class MergeableStoreViaGetPut[-K, V: Semigroup](store: Store[K, V], fc: FutureCollector = FutureCollector.default)
  extends MergeableStoreViaSingleGetPut[K, V](store) {

  override def multiGet[K1 <: K](ks: Set[K1]) = store.multiGet(ks)
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = store.multiPut(kvs)

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    MergeableStore.multiMergeFromMultiSet(this, kvs)(fc, semigroup)
  }
}
