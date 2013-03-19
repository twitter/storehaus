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

class UnpivotedMergeable[-K, OuterK, InnerK, V](wrapped: Mergeable[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK))
  extends Mergeable[K, V] {

  override def merge(pair: (K, V)): Future[Unit] = {
    val (k, v) = pair
    val (outerK, innerK) = split(k)
    wrapped.merge(outerK -> Map(innerK -> v))
  }

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = {
    val pivoted: Map[OuterK, Map[InnerK, V]] =
      CollectionOps.pivotMap[K1, OuterK, InnerK, V](kvs)(split)
    val ret: Map[OuterK, Future[Unit]] = wrapped.multiMerge(pivoted)
    kvs.flatMap {
      case (k, _) =>
        val (outerK, _) = split(k)
        (1 to pivoted(outerK).size).map { _ =>
          k -> ret(outerK)
        }
    }.toMap
  }
}
