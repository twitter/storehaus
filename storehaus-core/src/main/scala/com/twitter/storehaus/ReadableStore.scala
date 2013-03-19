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

object ReadableStore {
  implicit def enrich[K, V](store: ReadableStore[K, V]): EnrichedReadableStore[K, V] =
    new EnrichedReadableStore(store)

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
   * and returns the first values that are not exceptions.
   */
  def first[K, V](stores: Seq[ReadableStore[K, V]]): ReadableStore[K, V] = new ReplicatedReadableStore(stores)
  def fromMap[K, V](m: Map[K, V]): ReadableStore[K, V] = new MapStore(m)
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

  /**
   * Treat a PartialFunction like a ReadableStore
   */
  def fromPartial[K, V](getfn: PartialFunction[K, V]): ReadableStore[K, V] = new AbstractReadableStore[K, V] {
    override def get(k: K) = if (getfn.isDefinedAt(k)) {
      try { Future.value(Some(getfn(k))) }
      catch { case e: Throwable => Future.exception(e) }
    } else Future.None
  }

  def andThen[K, V, V2, V3 >: V](l: ReadableStore[K, V], r: ReadableStore[V3, V2])(implicit fc: FutureCollector[V3]): ReadableStore[K, V2] =
    new ComposedStore[K, V, V2, V3](l, r)

  def unpivot[K, OuterK, InnerK, V](store: ReadableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): ReadableStore[K, V] =
    new UnpivotedReadableStore(store)(split)

  def convert[K1, K2, V1, V2](store: ReadableStore[K1, V1])(kfn: K2 => K1)(vfn: V1 => Future[V2]): ReadableStore[K2, V2] =
    new ConvertedReadableStore(store)(kfn)(vfn)
}

trait ReadableStore[-K, +V] extends Closeable { self =>
  def get(k: K): Future[Option[V]] = multiGet(Set(k)).apply(k)

  /**
   * all keys in the set are in the resulting map
   */
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    ks.map { k => (k, self.get(k)) }.toMap

  override def close { }
}

// Abstract extension of the defined trait to minimize trait bloat.
abstract class AbstractReadableStore[-K, +V] extends ReadableStore[K, V]
