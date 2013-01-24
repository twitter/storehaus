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
  def empty[K,V]: ReadableStore[K,V] = new EmptyReadableStore[K, V]
}

trait ReadableStore[K,V] extends Closeable { self =>
  def get(k: K): Future[Option[V]] = multiGet(Set(k)) map { _.get(k).flatten.headOption }

  /**
   * A definitely present value is signaled by Some(v), while a missing
   * value is signaled by None. If a particular value is not known for
   * some reason (for example: store failure, cache miss within the store,
   * timeout, etc) the value will be missing from the map.
   */
  def multiGet(ks: Set[K]): Future[Map[K,Option[V]]] = {
    val keySeq = ks.toSeq
    val futures: Seq[Future[Option[V]]] = keySeq map { k => get(k) }
    Future.collect(futures) map { vals => (keySeq zip vals).toMap } // Seq[Option[K,V]]
  }

  /**
   * Returns a new ReadableStore[K, V] that queries both this and the other
   * ReadableStore[K,V] for get and multiGet and returns the first
   * future to succeed (following com.twitter.util.Future's "or" logic).
   */
  def or(other: ReadableStore[K,V]) =
    new ReadableStore[K,V] {
      override def get(k: K) = self.get(k) or other.get(k)
      override def multiGet(ks: Set[K]) = self.multiGet(ks) or other.multiGet(ks)
    }

  override def close { }
}

// Store is immutable by default.

object Store {
  // TODO: Move to some collection util.
  def zipWith[K, V](keys: Set[K])(lookup: K => V): Map[K, V] =
    keys.foldLeft(Map.empty[K, V]) { (m, k) => m + (k -> lookup(k)) }
}

trait Store[Self <: Store[Self,K,V], K, V] extends ReadableStore[K, V] {
  def -(k: K): Future[Self]
  def +(pair: (K,V)): Future[Self]
  def update(k: K)(fn: Option[V] => Option[V]): Future[Self] = {
    get(k) flatMap { opt: Option[V] =>
      fn(opt)
        .map { v => this + (k -> v) }
        .getOrElse(this - k)
    }
  }
}

object KeysetStore {
  def fromMap[K,V](m: Map[K,V]): KeysetStore[MapStore[K,V],K,V] = new MapStore(m)
}

trait KeysetStore[Self <: KeysetStore[Self,K,V],K,V] extends Store[Self,K,V] {
  def keySet: Set[K]
  def size: Int
}

// Used as a constraint annotation.
trait MutableStore[Self <: MutableStore[Self,K,V],K,V] extends Store[Self,K,V]

// Used as a constraint annotation.
trait ConcurrentMutableStore[Self <: ConcurrentMutableStore[Self,K,V],K,V] extends MutableStore[Self,K,V]
