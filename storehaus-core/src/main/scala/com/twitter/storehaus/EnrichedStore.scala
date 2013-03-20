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

/** Enrichment class to add method to Store[K, V]
 * TODO in scala 2.10 this should be a value class
 */
class EnrichedStore[-K, V](store: Store[K, V]) {
  def unpivot[CombinedK, InnerK, InnerV](split: CombinedK => (K, InnerK))(implicit ev: V <:< Map[InnerK, InnerV]): Store[CombinedK, InnerV] =
    Store.unpivot(store.asInstanceOf[Store[K, Map[InnerK, InnerV]]])(split)
}
