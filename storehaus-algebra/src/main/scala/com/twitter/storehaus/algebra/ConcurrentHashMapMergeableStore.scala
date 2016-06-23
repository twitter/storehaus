/*
 * Copyright 2014 Twitter Inc.
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
import com.twitter.util.Future
import java.util.concurrent.{ ConcurrentHashMap => JConcurrentHashMap }

object ConcurrentHashMapMergeableStore {
  def apply[K, V: Semigroup](): ConcurrentHashMapMergeableStore[K, V] =
    new ConcurrentHashMapMergeableStore[K, V](new JConcurrentHashMap[K, V])
}

/** A threadsafe local MergeableStore
 *  useful for testing or other local applications
 */
class ConcurrentHashMapMergeableStore[K, V](map: JConcurrentHashMap[K, V])(
  implicit val semigroup: Semigroup[V]) extends MergeableStore[K, V] {

  override def get(k: K): Future[Option[V]] = Future.value(Option(map.get(k)))
  override def put(kv: (K, Option[V])): Future[Unit] = kv match {
    case (k, Some(v)) => map.put(k, v); Future.Unit
    case (k, None) => map.remove(k); Future.Unit
  }

  override def merge(kv: (K, V)): Future[Option[V]] = {
    val (k, v) = kv
    Option(map.get(k)) match {
      case None =>
        if (Option(map.putIfAbsent(k, v)).isEmpty) Future.None
        else merge(kv)
      case Some(oldV) =>
        if (map.replace(k, oldV, semigroup.plus(oldV, v))) Future.value(Some(oldV))
        else merge(kv)
    }
  }
}
