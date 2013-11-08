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

import com.twitter.util.{ Duration, Future }

/** Main trait for mutable stores.
 * Instances may implement EITHER put, multiPut or both. The default implementations
 * are in terms of each other.
 */
trait WritableStore[-K, V] {
  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  def put(kv: (K, Option[V])): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
  /** Replace a set of keys at one time */
  def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, put(kv)) }
}

/**
 * Trait for building mutable store with TTL.
 */
trait WithPutTtl[K, V, S <: Store[K, V]] {
  def withPutTtl(ttl: Duration): S
}
