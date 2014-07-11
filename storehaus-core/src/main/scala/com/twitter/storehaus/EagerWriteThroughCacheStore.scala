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

import com.twitter.storehaus.cache.MutableCache
import com.twitter.util.{ Future, Return }
import scala.collection.breakOut

/*
  This will eagerly read/write to the backing gage when doing operations against its underlying store.
  It will not wait for the return of the network call to update the cache entry.
  The cache itself is assumed to be backed by a thread safe implementation.
*/
class EagerWriteThroughCacheStore[K, V](store: Store[K, V], threadSafeCache: MutableCache[K, Future[Option[V]]]) extends Store[K, V] {

  override def put(kv: (K, Option[V])): Future[Unit] = {
    threadSafeCache += (kv._1, Future.value(kv._2))
    store.put(kv)
  }

  override def get(k: K): Future[Option[V]] = {
    threadSafeCache.get(k).getOrElse {
      store.get(k)
    }
  }

  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] = {
    val present: Map[K1, Future[Option[V]]] =
      keys.map { k => k -> threadSafeCache.get(k) }.collect { case (k, Some(v)) => k -> v }(breakOut)

    val replaced = store.multiGet(keys -- present.keySet)

    replaced.foreach {kv =>
      threadSafeCache += ((kv._1, kv._2))
    }

    present ++ replaced
  }

 override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
  kvs.foreach { case (k, optiV) =>
    threadSafeCache += ((k, Future.value(optiV)))
  }
  store.multiPut(kvs)
 }
}