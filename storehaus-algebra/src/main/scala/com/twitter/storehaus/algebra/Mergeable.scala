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

import com.twitter.algebird.{ Monoid, StatefulSummer }
import com.twitter.bijection.ImplicitBijection
import com.twitter.util.Future

object Mergeable {
  def unpivot[K, OuterK, InnerK, V: Monoid](mergeable: Mergeable[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): Mergeable[K, V] =
    new UnpivotedMergeable(mergeable)(split)

  def withSummer[K, V](mergeable: Mergeable[K, V], summer: StatefulSummer[Map[K, V]]): Mergeable[K, V] =
    new BufferingMergeable(mergeable, summer)

  def convert[K1, K2, V1, V2](mergeable: Mergeable[K1, V1])(kfn: K2 => K1)
    (implicit bij: ImplicitBijection[V2, V1]): Mergeable[K2, V2] =
    new ConvertedMergeable(mergeable)(kfn)
}

trait Mergeable[-K, V] extends java.io.Serializable {
  def monoid: Monoid[V]
  def merge(kv: (K, V)): Future[Unit] = multiMerge(Map(kv)).apply(kv._1)
  def multiMerge[K1<:K](kvs: Map[K1,V]): Map[K1, Future[Unit]] = kvs.map { kv => (kv._1, merge(kv)) }
}
