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
import com.twitter.storehaus.{ CollectionOps, UnpivotedStore }
import com.twitter.util.{Future, Time}

/**
 * MergeableStore enrichment which presents a MergeableStore[K, V]
 * over top of a packed MergeableStore[OuterK, Map[InnerK, V]].
 *
 * @author Sam Ritchie
 */

class UnpivotedMergeableStore[-K, OuterK, InnerK, V: Semigroup](
  store: MergeableStore[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK)
) extends UnpivotedStore[K, OuterK, InnerK, V](store)(split) with MergeableStore[K, V] {

  override def semigroup: Semigroup[V] = implicitly[Semigroup[V]]

  override def merge(pair: (K, V)): Future[Option[V]] = {
    val (k, v) = pair
    val (outerK, innerK) = split(k)
    store.merge(outerK -> Map(innerK -> v))
      .map { _.flatMap { inner => inner.get(innerK) } }
  }

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    val pivoted: Map[OuterK, Map[InnerK, V]] =
      CollectionOps.pivotMap[K1, OuterK, InnerK, V](kvs)(split)
    val ret: Map[OuterK, Future[Option[Map[InnerK, V]]]] = store.multiMerge(pivoted)
    kvs.map {
      case (k, _) =>
        val (outerK, innerK) = split(k)
        k -> ret(outerK).map(_.flatMap { innerM => innerM.get(innerK) })
    }
  }
  override def close(t: Time): Future[Unit] = store.close(t)
}
