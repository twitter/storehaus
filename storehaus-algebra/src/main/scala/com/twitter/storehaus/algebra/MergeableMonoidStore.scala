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

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.storehaus.{ FutureCollector, Store }
import com.twitter.util.Future

/** Uses the monoid and filters zeros on put. On get, empty is replaced with Monoid.zero.
 * This store never returns Future.None. The design choice of never returning None is
 * to make sure users do not mistake zero for meaning empty. Clearly, we cannot tell the
 * difference, but returning zero is assumed to make it less likely the user thinks the
 * key is absent (which this store can never confirm).
 */
class MergeableMonoidStore[-K, V: Monoid](store: Store[K, V], fc: FutureCollector[(K, Option[V])] = FutureCollector.default[(K, Option[V])])
  extends MergeableStoreViaGetPut[K, V](store, fc) {
  override def semigroup: Semigroup[V] = implicitly[Monoid[V]]

  private def default: Some[V] = Some(Monoid.zero)

  private def orElse(opt: Option[V]): Some[V] =
    opt match {
      case s@Some(_) => s
      case None => default
    }

  override def get(k: K): Future[Some[V]] =
    store.get(k).map(orElse(_))

  override def multiGet[K1 <: K](ks: Set[K1]) =
    store.multiGet(ks).mapValues { futv => futv.map(_.orElse(default)) }

  override def put(kv: (K, Option[V])) = {
    val (k, optV) = kv
    store.put((k, optV.filter(Monoid.isNonZero(_))))
  }
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) =
    store.multiPut(kvs.mapValues(_.filter(v => Monoid.isNonZero(v))))
}
