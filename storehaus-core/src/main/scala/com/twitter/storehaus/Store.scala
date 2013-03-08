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

import com.twitter.util.Future
import java.io.Closeable
import java.util.{ Map => JMap }

object Store {
  def fromJMap[K, V](m: JMap[K, Option[V]]): Store[K, V] = new JMapStore[K, V] {
    override val jstore = m
  }

  def lru[K, V](maxSize: Int = 1000): Store[K, V] = new LRUStore(maxSize)

  /**
   * Returns a new Store[K, V] that queries all of the stores on read
   * and returns the first values that are not exceptions. Writes are
   * routed to every store in the supplied sequence.
   */
  def first[K, V](stores: Seq[Store[K, V]])(implicit collect: FutureCollector[Unit]): Store[K, V] =
    new ReplicatedStore(stores)
}

trait Store[-K, V] extends ReadableStore[K, V] with Closeable { self =>
  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  def put(kv: (K, Option[V])): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
  def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, put(kv)) }

  override def close { }
}
