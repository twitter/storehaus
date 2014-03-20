/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus


/**
 * @author Mansur Ashraf
 * @since 1/14/14
 */
trait QueryableStore[Q, +V] {

  /**
   * Returns a store which take Query Q as a key and returns a Seq of value matching that Query
   * @return
   */
  def queryable: ReadableStore[Q, Seq[V]]
}

object QueryableStore {
  implicit def enrich[K, V, Q](store: Store[K, V] with QueryableStore[Q, V]): EnrichedQueryableStore[K, V, Q] =
    new EnrichedQueryableStore(store)
}
