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

  /** Treat a Function1 like a ReadableStore
   */
  def fromFunction[K,V](getfn: (K) => Option[V]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = // I know Future(getfn(k)) looks similar, we've seen some high costs with that
      try { Future.value(getfn(k)) }
      catch { case e: Throwable => Future.exception(e) }
  }

  /** Treat a PartialFunction like a ReadableStore
   */
  def fromPartial[K,V](getfn: PartialFunction[K,V]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = if(getfn.isDefinedAt(k)) {
      try { Future.value(Some(getfn(k))) }
      catch { case e: Throwable => Future.exception(e) }
    } else Future.None
  }

  /** Treat a function returning a Future[Option[V]] as the get method of a ReadableStore
   */
  def fromFuture[K,V](getfn: (K) => Future[Option[V]]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = getfn(k)
  }
}

trait ReadableStore[-K, +V] extends Closeable { self =>
  def get(k: K): Future[Option[V]] = multiGet(Set(k)).apply(k)

  /**
   * all keys in the set are in the resulting map
   */
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    ks.map { k => (k, self.get(k)) }.toMap

  override def close { }

  // These methods are defined in terms of the above:
  /**
   * Look-up the V, and then use that as the key into the next store
   */
  def andThen[V2, V3 >: V](rs: ReadableStore[V3, V2])(implicit fc: FutureCollector[V3]): ReadableStore[K, V2] =
    new AbstractReadableStore[K, V2] {
      override def get(k: K) =
        for (
          optV <- self.get(k);
          v2 <- optV.map { v => rs.get(v) }.getOrElse(Future.None)
        ) yield v2

      override def multiGet[K1 <: K](ks: Set[K1]) = {
        val mapFV: Map[K1, Future[Option[V]]] = self.multiGet(ks)
        val fkVals: Future[Map[K1, Option[V]]] = FutureOps.mapCollect(mapFV)

        val fmapf: Future[Map[K1, Future[Option[V2]]]] = fkVals.map { k1vMap =>
          val vSet: Set[V] = k1vMap.view.flatMap { _._2 }.toSet
          val v2s: Map[V, Future[Option[V2]]] = rs.multiGet(vSet)
          k1vMap.mapValues { optV => optV.map { v2s(_) }.getOrElse(Future.None) }
        }
        FutureOps.liftValues(ks, fmapf).mapValues { _.flatten }
      }
      override def close { self.close; rs.close }
    }

  /**
   * convert K1 to K then lookup K in this store
   */
  def composeKeyMapping[K1](fn: (K1) => K): ReadableStore[K1, V] =
    new ConvertedReadableStore(self, fn, { (v: V) => Future.value(v) })

  /**
   * apply an async function on all the values
   */
  def flatMapValues[V2](fn: (V) => Future[V2]): ReadableStore[K, V2] =
    new ConvertedReadableStore(self, identity[K] _, fn)
  /**
   * Apply a non-blocking function on all the values. If this function throws, it will be caught in
   * the Future
   */
  def mapValues[V2](fn: (V) => V2): ReadableStore[K, V2] =
    flatMapValues(fn.andThen { Future.value(_) })
}

// Abstract extension of the defined trait to minimize trait bloat.
abstract class AbstractReadableStore[-K, +V] extends ReadableStore[K, V]
