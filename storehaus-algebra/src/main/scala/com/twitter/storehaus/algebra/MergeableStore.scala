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
import com.twitter.storehaus.{ FutureCollector, Store }
import com.twitter.util.Future

// Needed for the monoid on Future[V]
import com.twitter.algebird.util.UtilAlgebras._

/** a Store
 * None is indistinguishable from monoid.zero
 */
trait MergeableStore[-K, V] extends Mergeable[K,V] with Store[K, V]

abstract class AbstractMergeableStore[-K, V] extends MergeableStore[K, V]  {
  /** sets to monoid.plus(get(kv._1).get.getOrElse(monoid.zero), kv._2)
   * but maybe more efficient implementations
   */
  override def merge(kv: (K, V)): Future[Unit] =
    for(vOpt <- get(kv._1);
        oldV = vOpt.getOrElse(monoid.zero);
        newV = monoid.plus(oldV, kv._2);
        finalUnit <- put((kv._1, monoid.nonZeroOption(newV)))) yield finalUnit

  protected def futureCollector[K1 <: K]: FutureCollector[(K1,Option[V])] =
    FutureCollector.default[(K1,Option[V])]

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] =
    MergeableStore.multiMergeFromMultiSet(this, kvs)(futureCollector[K1], monoid)
}

object MergeableStore {
  /**
    * Implements multiMerge functionality in terms of an underlying
    * store's multiGet and multiSet.
    */
  def multiMergeFromMultiSet[K, V](store: Store[K, V], kvs: Map[K, V])
    (implicit collect: FutureCollector[(K, Option[V])], monoid: Monoid[V]): Map[K, Future[Unit]] = {
    val keySet = kvs.keySet
    val collected: Future[Map[K, Future[Unit]]] =
      collect {
        store.multiGet(keySet).map {
          case (k, futureOptV) =>
            futureOptV.map { v =>
              k -> Semigroup.plus(v, kvs.get(k)).flatMap { Monoid.nonZeroOption(_) }
            }
        }.toSeq
      }.map { pairs: Seq[(K, Option[V])] => store.multiPut(pairs.toMap) }
    keySet.map { k =>
      k -> collected.flatMap { _.apply(k) }
    }.toMap
  }

  def fromStore[K,V](store: Store[K,V])(implicit mon: Monoid[V], fc: FutureCollector[(K,Option[V])]): MergeableStore[K,V] =
    new AbstractMergeableStore[K,V] {
      val monoid = mon
      // FutureCollector is invariant, but clearly it will accept K1 <: K
      // and the contract is that it should not change K1 at all.
      override def futureCollector[K1] = fc.asInstanceOf[FutureCollector[(K1,Option[V])]]
      override def get(k: K) = store.get(k)
      override def multiGet[K1<:K](ks: Set[K1]) = store.multiGet(ks)
      override def put(kv: (K,Option[V])) = store.put(kv)
      override def multiPut[K1<:K](kvs: Map[K1,Option[V]]) = store.multiPut(kvs)
    }
}
