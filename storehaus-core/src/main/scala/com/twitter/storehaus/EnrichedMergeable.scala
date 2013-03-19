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

class EnrichedMergeable[-K, V](wrapped: Mergeable[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))
    (implicit ev: V <:< Map[InnerK, InnerV])
      : Mergeable[CombinedK, InnerV] =
    Mergeable.unpivot(wrapped.asInstanceOf[Mergeable[K, Map[InnerK, InnerV]]])(split)

  def composeKeyMapping[K1](fn: K1 => K): Mergeable[K1, V] =
    Mergeable.convert(wrapped)(fn)(identity)

  def mapValues[V1](fn: V1 => V): Mergeable[K, V1] =
    Mergeable.convert(wrapped)(identity[K])(fn)

  def convert[K1, V1](kfn: K1 => K)(vfn: V1 => V): Mergeable[K1, V1] =
    Mergeable.convert(wrapped)(kfn)(vfn)
}
