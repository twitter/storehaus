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
import com.twitter.bijection.{Bijection, ImplicitBijection, Injection}

import java.io.Closeable
import scala.math.Equiv

trait ReadableStore[K,+V] extends Closeable {
  def get(k: K): Future[V] = multiGet(Set(k)).apply(k)

  /**
   * all keys in the set are in the resulting map
   */
  def multiGet(ks: Set[K]): Map[K,Future[V]] = ks.map { k => (k, get(k)) }.toMap

  // Must be idempotent, closing twice is safe.
  override def close { }
}

abstract class AbstractReadableStore[K,+V] extends ReadableStore[K,V]

object ReadableStore {
  def none[K]: ReadableStore[K, Option[Nothing]] = new ConstReadableStore[K, Option[Nothing]](None)
  def const[K, V](v: V): ReadableStore[K, V] = new ConstReadableStore[K, V](v)
  /**
   * Returns a new ReadableStore[K, V] that queries both this and the other
   * ReadableStore[K,V] for get and multiGet and returns the first
   * future to succeed (following com.twitter.util.Future's "or" logic).
   */
  def first[K,V](stores: Seq[ReadableStore[K,V]]) = {
    import Store.{selectFirstSuccessfulTrial => selectFirst, sequenceMap}
    new AbstractReadableStore[K,V] {
      override def get(k: K) = selectFirst(stores.map { _.get(k) })

      override def multiGet(ks: Set[K]) = {
        sequenceMap(stores.map { _.multiGet(ks) })
          .mapValues { selectFirst(_) }
      }

      override def close { stores.foreach { _.close } }
    }
  }
  // TODO other combinators here:
  // for fault-tolerance: read until you have more than n results that match, then return that matched value
  // TODO add a mechanism to get callbacks when some keys disagree so we can monitor health
  def quorum[K,V](n: Int, stores: Seq[ReadableStore[K, V]])(implicit eq: Equiv[V]): ReadableStore[K, V] =
    TODO

  implicit def bijection[K1,K2,V1,V2](implicit kbij: ImplicitBijection[K1,K2], vbij:
  ImplicitBijection[V1,V2]): Bijection[ReadableStore[K1, V1], ReadableStore[K2, V2]] =
    TODO

  def orDefault[K, V](rs: ReadableStore[K, Option[V]], default: V): ReadableStore[K, V] = TODO
  def filterDefault[K, V](rs: ReadableStore[K, V], default: V)(implicit eq: Equiv[V]):
    ReadableStore[K, Option[V]] = TODO

  // We don't need a full bijection here, just an injection will do
  def pivot[K1,K2,K,V](rs: ReadableStore[K1, Map[K2, V]])(implicit inj: Injection[K, (K1,K2)]):
    ReadableStore[K, Option[V]] = TODO

  // Choose which of several stores to use:
  def shard[K1,K2,K,V](router: (K1) => ReadableStore[K2, V])(implicit inj: Injection[K, (K1,K2)]):
    ReadableStore[K, V] = TODO

  def sequence[K, V](stores: Seq[ReadableStore[K, V]]): ReadableStore[K, Seq[V]] = TODO

  def TODO = sys.error("TODO")
}

