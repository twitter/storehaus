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

package com.twitter.storehaus.algebra

import com.twitter.algebird.{ Monoid, Semigroup, StatefulSummer }
import com.twitter.util.Future

class BufferingStore[-K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]]) extends MergeableStore[K, V] {
  override implicit def monoid: Monoid[V] = store.monoid

  protected def flushBy[T](fn: Option[Map[K, V]] => T): T = {
    val flushed = summer.flush
    try {
      fn(flushed)
    } finally {
      flushed.foreach { store.multiMerge(_) }
    }
  }

  protected def selectAndSum(pair: (K, Option[V]), m: Option[Map[K, V]]): Option[V] =
    m.flatMap { partials =>
      Semigroup.plus(pair._2, partials.get(pair._1))
    }

  override def get(k: K): Future[Option[V]] =
    flushBy { flushed =>
      store.get(k).map { v => selectAndSum((k, v), flushed) }
    }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    flushBy { flushed =>
      store.multiGet(ks).map { case (k, futureV) =>
          k -> futureV.map { v => selectAndSum((k, v), flushed) }
      }
    }

  override def merge(pair: (K, V)): Future[Unit] = multiMerge(Map(pair))(pair._1)

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] =
    summer.put(kvs.asInstanceOf[Map[K, V]])
      .map { store.multiMerge(_).asInstanceOf[Map[K1, Future[Unit]]] }
      .getOrElse(kvs mapValues { _ => Future.Unit })
}
