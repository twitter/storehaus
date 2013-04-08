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

import com.twitter.storehaus.cache.{ Cache, MutableCache }
import com.twitter.util.Future
import java.io.Closeable

/** Holds various factory and transformation functions for ReadableStore instances */
object ReadableStore {
  /** Adds enrichment methods to ReadableStore
   * This allows you to do: store.mapValues, for instance.  Access this in your code
   * by doing: {{{ import ReadableStore.enrich }}}
   */
  implicit def enrich[K, V](store: ReadableStore[K, V]): EnrichedReadableStore[K, V] =
    new EnrichedReadableStore(store)

  /** Due to the variance notations, this suffices for an empty store of any K,V type */
  val empty: ReadableStore[Any, Nothing] = EmptyReadableStore

  /**
   * Returns a new ReadableStore[Any, V] that always returns the
   * supplied value V.
   */
  def const[V](v: V): ReadableStore[Any, V] = new AbstractReadableStore[Any, V] {
    override def get(k: Any) = Future.value(Some(v))
  }

  /**
   * Returns a new ReadableStore[K, V] that queries all of the stores
   * and returns the first values that are not exceptions and that
   * pass the supplied predicate.
   */
  def select[K, V](stores: Seq[ReadableStore[K, V]])(pred: Option[V] => Boolean): ReadableStore[K, V] =
    new ReplicatedReadableStore(stores)(pred)

  /**
   * Returns a ReadableStore[K, V] that attempts reads out of the
   * supplied Seq[ReadableStore[K, V]] in order and returns the first
   * successful value that passes the supplied predicate.
   */
  def find[K, V](stores: Seq[ReadableStore[K, V]])(pred: Option[V] => Boolean): ReadableStore[K, V] =
    new SearchingReadableStore(stores)(pred)

  /**
   * Returns a new ReadableStore[K, V] that queries all of the stores
   * and returns the first values that are not exceptions.
   */
  def first[K, V](stores: Seq[ReadableStore[K, V]]): ReadableStore[K, V] =
    select(stores)(_ => true)

  /**
   * Returns a new ReadableStore[K, V] that queries all of the stores
   * and returns the first values that are not exceptions and that are
   * present (ie, not equivalent to Future.None).
   */
  def firstPresent[K, V](stores: Seq[ReadableStore[K, V]]): ReadableStore[K, V] =
    select(stores)(_.isDefined)

  /** Factory method to create a ReadableStore from a Map. */
  def fromMap[K, V](m: Map[K, V]): ReadableStore[K, V] = new MapStore(m)

  /** Factory method to create a ReadableStore from an IndexedSeq. */
  def fromIndexedSeq[T](iseq: IndexedSeq[T]): ReadableStore[Int, T] = new IndexedSeqReadableStore(iseq)

  /**
   * Treat a Function1 like a ReadableStore
   */
  def fromFn[K, V](getfn: (K) => Option[V]): ReadableStore[K, V] = new AbstractReadableStore[K, V] {
    override def get(k: K) = // I know Future(getfn(k)) looks similar, we've seen some high costs with that
      try { Future.value(getfn(k)) }
      catch { case e: Throwable => Future.exception(e) }
  }

  /**
   * Treat a function returning a Future[Option[V]] as the get method of a ReadableStore
   */
  def fromFnFuture[K, V](getfn: (K) => Future[Option[V]]): ReadableStore[K, V] = new AbstractReadableStore[K, V] {
    override def get(k: K) = getfn(k)
  }

  /** Treat a PartialFunction like a ReadableStore
   */
  def fromPartial[K, V](getfn: PartialFunction[K, V]): ReadableStore[K, V] = new AbstractReadableStore[K, V] {
    override def get(k: K) = if (getfn.isDefinedAt(k)) {
      try { Future.value(Some(getfn(k))) }
      catch { case e: Throwable => Future.exception(e) }
    } else Future.None
  }

  /** Do a "join" on two stores: look up from the first, and use that value as the key in the next
   * A factory method for [[com.twitter.storehaus.ComposedStore]].
   * See also [[ReadableStore.convert]] if you need to change the value or key before the andThen
   */
  def andThen[K, V, V2, V3 >: V](l: ReadableStore[K, V], r: ReadableStore[V3, V2])(implicit fc: FutureCollector[V3]): ReadableStore[K, V2] =
    new ComposedStore[K, V, V2, V3](l, r)

  /** unpivot or uncurry a ReadableStore which has a value that is a Map.
   * Often it is more efficient to pack values into inner maps, especially when you have very sparse
   * stores.  This allows you to work with such a packed store as though it was unpacked
   */
  def unpivot[K, OuterK, InnerK, V](store: ReadableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): ReadableStore[K, V] =
    new UnpivotedReadableStore(store)(split)

  /** Lazily change the key and value for a store.
   * This does not change the representation, only alters before going in or out of the store.
   */
  def convert[K1, K2, V1, V2](store: ReadableStore[K1, V1])(kfn: K2 => K1)(vfn: V1 => Future[V2]): ReadableStore[K2, V2] =
    new ConvertedReadableStore(store)(kfn)(vfn)

  /* Returns a new ReadableStore that caches reads from the underlying
   * store using the supplied mutable cache.  */
  def withCache[K, V](store: ReadableStore[K, V], cache: MutableCache[K, Future[Option[V]]]): ReadableStore[K, V] =
    new CachedReadableStore(store, cache)

  /* Returns a new ReadableStore that caches reads from the underlying
   * store using the supplied immutable cache.  */
  def withCache[K, V](store: ReadableStore[K, V], cache: Cache[K, Future[Option[V]]]): ReadableStore[K, V] =
    new CachedReadableStore(store, cache.toMutable())
}

/** Main trait to represent asynchronous readable stores
 * Here you see the tri-state logic:
 * <ul>
 *   <li>Future(Some(v)) - The store has the item</li>
 *   <li>Future(None) - The store definitely DOES NOT have the item (not the same as no answer).</li>
 *   <li>Future.exception - Some kind of unexpected failure (including non-answer).</li>
 * </ul>
 */
trait ReadableStore[-K, +V] extends Closeable { self =>
  /** get a single key from the store.
   * Prefer multiGet if you are getting more than one key at a time
   */
  def get(k: K): Future[Option[V]] = multiGet(Set(k)).apply(k)

  /** Get a set of keys from the store.
   * Important: all keys in the input set are in the resulting map. If the store
   * fails to return a value for a given key, that should be represented by a
   * Future.exception.
   */
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    ks.map { k => (k, self.get(k)) }.toMap

  /** Close this store and release any resources.
   * It is undefined what happens on get/multiGet after close
   */
  override def close { }
}

/** Abstract extension of the defined trait to minimize trait bloat.
  * Probably mostly useful from Java or in anonymous instances whose return types
  * are constrained to be ReadableStore. Try not to expose AbstractReadableStore in
  * your APIs.
  */
abstract class AbstractReadableStore[-K, +V] extends ReadableStore[K, V]
