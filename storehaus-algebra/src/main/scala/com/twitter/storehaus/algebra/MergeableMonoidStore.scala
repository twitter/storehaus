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
import com.twitter.storehaus.{ FutureCollector, Store }
import com.twitter.util.Future

class MergeableMonoidStore[-K, V: Monoid](store: Store[K, V], fc: FutureCollector[(K, Option[V])] = FutureCollector.default[(K, Option[V])])
  extends MergeableStore[K, V] {
  override val monoid: Monoid[V] = implicitly[Monoid[V]]

  override def get(k: K) = store.get(k)
  override def multiGet[K1 <: K](ks: Set[K1]) = store.multiGet(ks)
  override def put(kv: (K, Option[V])) = store.put(kv)
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = store.multiPut(kvs)

  /**
   * sets to monoid.plus(get(kv._1).get.getOrElse(monoid.zero), kv._2)
   * but maybe more efficient implementations
   */
  override def merge(kv: (K, V)): Future[Option[V]] =
    for {
      vOpt <- get(kv._1)
      oldV = vOpt.getOrElse(Monoid.zero[V])
      newV = Monoid.plus(oldV, kv._2)
      finalUnit <- put((kv._1, Monoid.nonZeroOption(newV)))
    } yield vOpt

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    // FutureCollector is invariant, but clearly it will accept K1 <: K
    // and the contract is that it should not change K1 at all.
    implicit val collector = fc.asInstanceOf[FutureCollector[(K1, Option[V])]]
    MergeableStore.multiMergeFromMultiSet(this, kvs)
  }
}
