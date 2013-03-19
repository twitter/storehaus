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

trait MergeableStore[-K, V] extends Mergeable[K,V] with Store[K, V]

object MergeableStore {
  implicit def enrich[K, V](store: MergeableStore[K, V]): EnrichedMergeableStore[K, V] =
    new EnrichedMergeableStore(store)

  def unpivot[K, OuterK, InnerK, V](store: MergeableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): MergeableStore[K, V] =
    new UnpivotedMergeableStore(store)(split)
}
