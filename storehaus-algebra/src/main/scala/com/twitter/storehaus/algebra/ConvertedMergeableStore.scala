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
import com.twitter.bijection.{ Conversion, Injection, ImplicitBijection }
import com.twitter.util.Future

import Conversion.asMethod

/**
  * Caveat Emptor! The monoid on the resulting
  * `ConvertedMergeableStore` will be the bijected monoid from
  * Monoid[V1] => Monoid[V2]. This will not necessarily result in the
  * behavior you'd expect.
  */
class ConvertedMergeableStore[K1, -K2, V1, V2](store: MergeableStore[K1, V1])(kfn: K2 => K1)
  (implicit bij: ImplicitBijection[V2, V1])
  extends com.twitter.storehaus.ConvertedStore[K1, K2, V1, V2](store)(kfn)(Injection.fromBijection(bij.bijection))
  with MergeableStore[K2, V2] {
  import com.twitter.algebird.bijection.AlgebirdBijections._

  override def monoid: Monoid[V2] = store.monoid.as[Monoid[V2]]

  override def merge(kv: (K2, V2)): Future[Unit] = {
    val k1 = kfn(kv._1)
    val v1 = bij.bijection(kv._2)
    store.merge((k1, v1))
  }

  override def multiMerge[K3 <: K2](kvs: Map[K3, V2]): Map[K3, Future[Unit]] = {
    val mapK1V1 = kvs.map { case (k3, v2) => (kfn(k3), bij.bijection(v2)) }
    val res: Map[K1, Future[Unit]] = store.multiMerge(mapK1V1)
    kvs.keySet.map { k3 => (k3, res(kfn(k3))) }.toMap
  }

  override def close { store.close }
}
