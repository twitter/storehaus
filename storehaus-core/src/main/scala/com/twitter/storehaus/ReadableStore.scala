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

import com.twitter.util.{Future, Throw, Return}
import java.io.Closeable

object ReadableStore {
  val empty: ReadableStore[Any, Nothing] = EmptyReadableStore
  def const[V](v: V): ReadableStore[Any, V] = new AbstractReadableStore[Any, V] {
      override def get(k: Any) = Future.value(Some(v))
  }
  /**
   * Returns a new ReadableStore[K, V] that queries all of the stores
   * and returns the first values that are not exceptions.
   */
  def first[K,V](stores: Seq[ReadableStore[K, V]]): ReadableStore[K, V]
    = new ReplicatedReadableStore(stores)

  // This should go somewhere else, but it is needed for many combinators on stores
  def combineMaps[K,V](m: Seq[Map[K,V]]): Map[K,Seq[V]] =
    m.foldLeft(Map[K,List[V]]()) { (oldM, mkv) =>
      mkv.foldLeft(oldM) { (seqm, kv) =>
        seqm + (kv._1 -> (kv._2 :: seqm.getOrElse(kv._1, Nil)))
      }
    }.mapValues { _.reverse }
}

trait ReadableStore[-K,+V] extends Closeable { self =>
  def get(k: K): Future[Option[V]] = multiGet(Set(k)).apply(k)

  /**
   * all keys in the set are in the resulting map
   */
  def multiGet[K1<:K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    ks.map { k => (k, self.get(k)) }.toMap

  override def close { }

  // These methods are defined in terms of the above:
  /** Look-up the V, and then use that as the key into the next store
  */
  def andThen[V2,V3>:V](rs: ReadableStore[V3,V2])(implicit fc: FutureCollector[V3]): ReadableStore[K,V2] =
    new AbstractReadableStore[K,V2] {
      override def get(k: K) = for(optV <- self.get(k);
        v2 <- optV.map { v => rs.get(v) }.getOrElse(Future.None)) yield v2

      override def multiGet[K1<:K](ks: Set[K1]) = {
        val mapFV: Map[K1,Future[Option[V]]] = self.multiGet(ks)
        val fkVals: Future[Map[K1,Option[V]]] = Store.mapCollect(mapFV)

        val fmapf: Future[Map[K1,Future[Option[V2]]]] = fkVals.map { k1vMap =>
          val vSet: Set[V] = k1vMap.view.flatMap { _._2 }.toSet
          val v2s: Map[V,Future[Option[V2]]] = rs.multiGet(vSet)
          k1vMap.mapValues { optV => optV.map { v2s(_) }.getOrElse(Future.None) }
        }
        Store.liftValues(ks, fmapf).mapValues { _.flatten }
      }
      override def close { self.close; rs.close }
    }

  /** convert K1 to K then lookup K in this store
   */
  def composeKeyMapping[K1](fn: (K1) => K): ReadableStore[K1,V] =
    new ConvertedReadableStore(self, fn, {(v: V) => Future.value(v)})

  /** apply an async function on all the values
   */
  def flatMapValues[V2](fn: (V) => Future[V2]): ReadableStore[K,V2] =
    new ConvertedReadableStore(self, identity[K] _, fn)
  /** Apply a non-blocking function on all the values. If this function throws, it will be caught in
   * the Future
   */
  def mapValues[V2](fn: (V) => V2): ReadableStore[K,V2] =
    flatMapValues(fn.andThen { Future.value(_) })
}

// For teh Java
abstract class AbstractReadableStore[-K,+V] extends ReadableStore[K,V]

/**
 * Value conversion returns a Future because V1 => V2 may fail, and we are going to convert to a
 * future anyway (so, a Try is kind of a Future that is not async). Thus we might as well add the
 * flexibility of accepting a V1 => Future[V2], though in practice Future.value/exception will
 * generally be used
 */
class ConvertedReadableStore[K1,-K2,V1,+V2](rs: ReadableStore[K1,V1], kfn: K2 => K1, vfn: V1 => Future[V2]) extends
  ReadableStore[K2,V2] {
  override def get(k2: K2) = Store.flatMapValue(rs.get(kfn(k2)))(vfn)
  override def multiGet[K3<:K2](s: Set[K3]) = {
    val k1ToV2 = rs.multiGet(s.map(kfn)).mapValues(Store.flatMapValue(_)(vfn))
    s.map { k3 => (k3, k1ToV2(kfn(k3))) }.toMap
  }
  override def close { rs.close }
}
