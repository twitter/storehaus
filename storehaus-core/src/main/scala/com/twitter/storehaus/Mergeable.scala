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

object Mergeable {
  implicit def enrich[K, V](store: Mergeable[K, V]): EnrichedMergeable[K, V] =
    new EnrichedMergeable[K, V](store)

  def unpivot[K, OuterK, InnerK, V](mergeable: Mergeable[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): Mergeable[K, V] =
    new UnpivotedMergeable(mergeable)(split)
}

trait Mergeable[-K, V] extends java.io.Serializable {
  def merge(kv: (K, V)): Future[Unit] = multiMerge(Map(kv)).apply(kv._1)
  def multiMerge[K1<:K](kvs: Map[K1,V]): Map[K1, Future[Unit]] = kvs.map { kv => (kv._1, merge(kv)) }
}
