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

import com.twitter.util.{Future, Throw, Return}

import com.twitter.algebird.{Semigroup}

trait MergeableStore[K, V] extends ReadableStore[K, V] {
  def semigroup: Semigroup[V]
  /** Returns the value just BEFORE this operation
   * the return value is Future(vbefore)
   * the final stored value is: semigroup.plus(vbefore, kv._1)
   */
  def add(kv: (K,V)): Future[V] = multiAdd(Map(kv)).apply(kv._1)
  // May be more efficient version of the above
  def multiAdd(kvs: Map[K,V]): Map[K,Future[V]] = kvs.map { kv => (kv._1, add(kv)) }
}

object MergeableStore {
  // Combinators:
}
