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

/**
 *  @author Oscar Boykin
 */

object ShardedReadableStore {
  def fromMap[K1,K2,V](m: Map[K1, ReadableStore[K2, V]]): ReadableStore[(K1,K2), V] =
    new ShardedReadableStore[K1, K2, V, ReadableStore[K2, V]] {
      def getRoute(k1: K1) = m.get(k1)
      override def close { m.values.foreach { _.close } }
    }
  // You can imagine a version that takes a routing function that might
  // even be dynamic in time, but this is unimplemented for now
}

/** combines a mapping of ReadableStores into one ReadableStore that internally routes
 * Note: if K1 is absent from the routes, you will always get Future.None as a result.
 * you may want to combine this with ReadableStore.composeKeyMapping the change (K => (K1,K2))
 *
 */
abstract class ShardedReadableStore[-K1, -K2, +V, +S <: ReadableStore[K2, V]] extends ReadableStore[(K1,K2), V] {
  def getRoute(k1: K1): Option[S]

  override def get(k: (K1,K2)): Future[Option[V]] = {
    val (k1, k2) = k
    getRoute(k1) match {
      case Some(rs) => rs.get(k2)
      case None => Future.None
    }
  }

  override def multiGet[T<:(K1,K2)](ks: Set[T]): Map[T, Future[Option[V]]] = {
    // Do the lookup:
    val ksMap: Map[K1, Map[K2, Future[Option[V]]]] = ks.groupBy { _._1 }
      .mapValues { sett =>
        val k1 = sett.head._1 // sett is never empty
        getRoute(k1) match {
          // There is some store for this:
          case Some(rs) => rs.multiGet(sett.map { _._2 })
          // This whole key subspace is missing:
          case None => sett.map { t => (t._2, Future.None) }.toMap
        }
      }
    // Now construct the result map:
    Store.zipWith(ks) { t => ksMap(t._1)(t._2) }
  }
}

object ShardedStore {
  def fromMap[K1,K2,V](m: Map[K1, Store[K2, V]]): Store[(K1,K2), V] =
    new ShardedStore[K1, K2, V, Store[K2, V]] {
      def getRoute(k1: K1) = m.get(k1)
      override def close { m.values.foreach { _.close } }
    }
}

/** combines a mapping of Stores into one Store that internally routes
 * Note: if a K1 is absent from the routes, any put will give a Future.exception
 */
abstract class ShardedStore[-K1,-K2,V, +S <: Store[K2,V]] extends ShardedReadableStore[K1,K2,V,S] with Store[(K1,K2), V] {
  override def put(kv: ((K1,K2), Option[V])): Future[Unit] = {
    val (k1, k2) = kv._1
    getRoute(k1) match {
      case Some(store) => store.put(k2, kv._2)
      case None => Future.exception(new MissingValueException(k1))
    }
  }
  override def multiPut[T<:(K1,K2)](kvs: Map[T, Option[V]]): Map[T, Future[Unit]] = {
    // Do the lookup:
    val ks = kvs.keys
    val fuMap: Map[K1, Map[K2, Future[Unit]]] = ks.groupBy { _._1 }
      .mapValues { sett =>
        val k1 = sett.head._1 // sett is never empty
        getRoute(k1) match {
          // There is some store for this:
          case Some(store) =>
            val subMap = sett.map { t => (t._2, kvs(t)) }.toMap
            store.multiPut(subMap)
          // This whole key subspace is missing:
          case None =>
            val ex = Future.exception(new MissingValueException(k1))
            sett.map { t => (t._2, ex) }.toMap
        }
      }
    // Now construct the result map:
    Store.zipWith(ks) { t => fuMap(t._1)(t._2) }
  }
}
