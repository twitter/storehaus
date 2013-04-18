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
import com.twitter.util.{Duration, Future, Timer}
import java.io.Closeable
import java.util.{ Map => JMap }

/** Factory methods and some combinators on Stores */
object Store {
  /** Import Store.enrich to get access to additional methods/combinators on your stores
   * see [[com.twitter.storehaus.EnrichedStore]]
   */
  implicit def enrich[K, V](store: Store[K, V]): EnrichedStore[K, V] =
    new EnrichedStore[K, V](store)

  /** Given a java Map with Option[V] for the values, make a store */
  def fromJMap[K, V](m: JMap[K, Option[V]]): Store[K, V] = new JMapStore[K, V] {
    override val jstore = m
  }

  /** Generates a store from the supplied
    * [[com.twitter.storehaus.cache.MutableCache]] */
  def fromCache[K, V](cache: MutableCache[K, V]) = new CacheStore(cache)

  /**
   * Returns a new Store[K, V] that queries all of the stores and
   * returns the first values that are not exceptions and that pass
   * the supplied predicate. Writes are routed to every store in the
   * supplied sequence.
   */
  def select[K, V](stores: Seq[Store[K, V]])(pred: Option[V] => Boolean)(implicit collect: FutureCollector[Unit]): Store[K, V] =
    new ReplicatedStore(stores)(pred)

  /**
   * Returns a new Store[K, V] that queries all of the stores on read
   * and returns the first values that are not exceptions. Writes are
   * routed to every store in the supplied sequence.
   */
  def first[K, V](stores: Seq[Store[K, V]])(implicit collect: FutureCollector[Unit]): Store[K, V] =
    select(stores)(_ => true)

  /**
   * Returns a new ReadableStore[K, V] that queries all of the stores
   * and returns the first values that are not exceptions and that are
   * present (ie, not equivalent to Future.None). Writes are routed to
   * every store in the supplied sequence.
   */
  def firstPresent[K, V](stores: Seq[Store[K, V]]): Store[K, V] =
    select(stores)(_.isDefined)

  /** Given a Store with values which are themselves Maps, unpivot/uncurry.
   * The values of the new store are the inner values of the original store.
   * multiGet/multiPut on this store will do the smart thing and pack into a
   * minimum number of multiGets/multiPuts on the underlying store
   */
  def unpivot[K, OuterK, InnerK, V](store: Store[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): Store[K, V] =
    new UnpivotedStore(store)(split)

  /**
   * Returns a Store[K, V] that attempts reads from a store multiple
   * times until a predicate is met or timeout after a couple of retries.
   */
  def withRetry[K, V](store: Store[K, V], backoffs: Stream[Duration])(pred: Option[V] => Boolean)(implicit timer: Timer): Store[K, V] =
    new RetriableStore(store, backoffs)(pred)
}

/** Main trait for mutable stores.
 * Instances must implement EITHER put, multiPut or both. The default implementations
 * are in terms of each other.
 */
trait Store[-K, V] extends ReadableStore[K, V] { self =>
  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  def put(kv: (K, Option[V])): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
  /** Replace a set of keys at one time */
  def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, put(kv)) }
}
