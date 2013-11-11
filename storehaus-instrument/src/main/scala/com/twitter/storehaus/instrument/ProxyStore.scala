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
package com.twitter.storehaus.instrument

import com.twitter.storehaus.{ AbstractReadableStore, Store, ReadableStore }
import com.twitter.util.{ Future, Time }

/** Defines the base of a proxy for a given type.
 *  A proxy acts a simple forwarder for type T
 *  an an instance of type T. This allows for further
 *  extention without the need to implement all of
 *  type T's methods
 */
// todo: move this into storehaus-core maybe?
trait ProxyStore[T] {
  protected def self: T
}

/** A StoreProxy interface for ReadableStores */
trait ReadableStoreProxy[K, V]
  extends ReadableStore[K, V]
     with ProxyStore[ReadableStore[K, V]] {
  override def get(k: K): Future[Option[V]] =
    self.get(k)
  override def multiGet[K1 <: K](
    ks: Set[K1]): Map[K1, Future[Option[V]]] =
    self.multiGet(ks)
  override def close(time: Time) = self.close(time)
}

/** A ProxyStore interface for Stores which may be
 *  both read from and written to */
// fixme: can we make this work by mixing in ReadableStoreProxy
//        currently the type of self would be a ReadableStore
//        if we did so
trait StoreProxy[K, V]
  extends ProxyStore[Store[K, V]]
     with Store[K, V] {
  override def put(kv: (K, Option[V])): Future[Unit] =
    self.put(kv)
  override def multiPut[K1 <: K](
    kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    self.multiPut(kvs)
  override def get(k: K): Future[Option[V]] =
    self.get(k)
  override def multiGet[K1 <: K](
    ks: Set[K1]): Map[K1, Future[Option[V]]] =
    self.multiGet(ks)
  override def close(time: Time) = self.close(time: Time)
}
