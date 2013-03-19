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

import com.twitter.algebird.StatefulSummer
import com.twitter.storehaus.Mergeable

object MergeableAlgebra {
  implicit def enrich[K, V](store: Mergeable[K, V]): AlgebraicMergeable[K, V] =
    new AlgebraicMergeable(store)

  def withSummer[K, V](mergeable: Mergeable[K, V], summer: StatefulSummer[Map[K, V]]): Mergeable[K, V] =
    new BufferingMergeable(mergeable, summer)
}

class AlgebraicMergeable[K, V](wrapped: Mergeable[K, V]) {
  def withSummer(summer: StatefulSummer[Map[K, V]]): Mergeable[K, V] =
    MergeableAlgebra.withSummer(wrapped, summer)
}
