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
import com.twitter.storehaus.Proxied
import com.twitter.util.{ Future, Time }

/** Proxy for Mergeables. Methods not overrided in extensions will be forwared to Proxied
 *  self member */
trait MergeableProxy[K, V] extends Proxied[Mergeable[K,V]] with Mergeable[K, V] {
  override def semigroup: Semigroup[V] = self.semigroup
  override def merge(kv: (K, V)): Future[Option[V]] = self.merge(kv)
  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = self.multiMerge(kvs)
  override def close(time: Time) = self.close(time)
}

/** Proxy for MergeableStoress. Methods not overrided in extensions will be forwared to Proxied
 *  self member */
trait MergeableStoreProxy[K, V] extends Proxied[MergeableStore[K, V]] with MergeableStore[K, V] {
  override def semigroup: Semigroup[V] = self.semigroup
  override def merge(kv: (K, V)): Future[Option[V]] = self.merge(kv)
  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = self.multiMerge(kvs)
  override def put(kv: (K, Option[V])): Future[Unit] = self.put(kv)
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = self.multiPut(kvs)
  override def get(k: K): Future[Option[V]] = self.get(k)
  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = self.multiGet(ks)
  override def close(time: Time) = self.close(time)
}
