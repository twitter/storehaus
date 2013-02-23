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
}

// For teh Java
abstract class AbstractReadableStore[-K,+V] extends ReadableStore[K,V]
