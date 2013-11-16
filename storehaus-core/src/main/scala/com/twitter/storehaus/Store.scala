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

import com.twitter.bijection.Injection
import com.twitter.storehaus.cache.MutableCache
import com.twitter.util.{ Duration, Future, Timer }
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
   * Returns a new Store[K, V] that queries all of the stores
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

  def convert[K1, K2, V1, V2](store: Store[K1, V1])(kfn: K2 => K1)(implicit inj: Injection[V2, V1]): Store[K2, V2] =
    new ConvertedStore(store)(kfn)(inj)

  /**
   * Returns a Store[K, V] that attempts reads from a store multiple
   * times until a predicate is met. The iterable of backoffs defines
   * the time interval between two read attempts. If there is not
   * read result satisfying the given predicate after all read
   * attempts, a NotFoundException will be thrown.
   */
  def withRetry[K, V](store: Store[K, V], backoffs: Iterable[Duration])(pred: Option[V] => Boolean)(implicit timer: Timer): Store[K, V] =
    new RetryingStore(store, backoffs)(pred)
}

/** Main trait for stores can be both read from and written to
 */
trait Store[-K, V] extends ReadableStore[K, V] with WritableStore[K, Option[V]]
