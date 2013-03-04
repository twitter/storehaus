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

import com.twitter.algebird.{Monoid, MapMonoid, Semigroup, MapAlgebra}
import com.twitter.storehaus._

import Store.mapCollect

import com.twitter.algebird.util.UtilAlgebras._

import com.twitter.util.Future

// Needed for the monoid on Future[V]
import com.twitter.algebird.util.UtilAlgebras._

trait Mergeable[-K, V] extends java.io.Serializable {
  def monoid: Monoid[V]
  def merge(kv: (K, V)): Future[Unit] =
    multiMerge(Map(kv)).apply(kv._1)

  def multiMerge[K1<:K](kvs: Map[K1,V]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, merge(kv)) }
}

/** a Store
 * None is indistinguishable from monoid.zero
 */
trait MergeableStore[-K, V] extends Mergeable[K,V] with Store[K, V] {
  /** sets to monoid.plus(get(kv._1).get.getOrElse(monoid.zero), kv._2)
   * but maybe more efficient implementations
   */
  override def merge(kv: (K, V)): Future[Unit] =
    for(vOpt <- get(kv._1);
        oldV = vOpt.getOrElse(monoid.zero);
        newV = monoid.plus(oldV, kv._2);
        finalUnit <- put((kv._1, monoid.nonZeroOption(newV)))) yield finalUnit
  // TODO: implement multiMerge in terms of multiGet/Put
}

object MergeableStore {
  def fromStore[K,V:Monoid](store: Store[K,V]): MergeableStore[K,V] = new MergeableStore[K,V] {
    val monoid = implicitly[Monoid[V]]
    override def get(k: K) = store.get(k)
    override def multiGet[K1<:K](ks: Set[K1]) = store.multiGet(ks)
    override def put(kv: (K,Option[V])) = store.put(kv)
    override def multiPut[K1<:K](kvs: Map[K1,Option[V]]) = store.multiPut(kvs)
  }
}
