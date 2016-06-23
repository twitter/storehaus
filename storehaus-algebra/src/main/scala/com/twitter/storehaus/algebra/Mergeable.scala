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
import com.twitter.util.{Closable, Future, Time}

/** Main trait to represent objects that are used for aggregation
 * */
trait Mergeable[-K, V] extends Closable {
  /** The semigroup equivalent to the merge operation of this store */
  def semigroup: Semigroup[V]
  /** Returns the value JUST BEFORE the merge. If it is empty, it is like a zero.
   * the key should hold:
   * val (k,v) = kv
   * result = get(k)
   * key is set to: result.map(Semigroup.plus(_, Some(v)).getOrElse(v) after this.
   */
  def merge(kv: (K, V)): Future[Option[V]] = multiMerge(Map(kv)).apply(kv._1)
  /** merge a set of keys. */
  def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] =
    kvs.map { kv => (kv._1, merge(kv)) }

  /** Close this store and release any resources.
   * It is undefined what happens if you merge after close
   */
  override def close(time: Time): Future[Unit] = Future.Unit
}
