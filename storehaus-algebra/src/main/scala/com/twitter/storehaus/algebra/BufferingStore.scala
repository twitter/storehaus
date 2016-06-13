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
import com.twitter.util.{ Future, Promise, Time }
import com.twitter.storehaus.{ FutureOps, FutureCollector, Store }
import scala.collection.mutable.{ Map => MMap }
import scala.collection.breakOut

/** For any given V, construct a StatefulSummer of Map[K, V]
 */
trait SummerConstructor[K] {
  def apply[V](mon: Semigroup[V]): StatefulSummer[Map[K, V]]
}

/** A Mergeable that sits on top of another mergeable and pre-aggregates before pushing into merge/multiMerge
 * This is very useful for cases where you have some keys that are very hot, or you have a remote mergeable that
 * you don't want to constantly hit.
 */
class BufferingMergeable[K, V](store: Mergeable[K, V], summerCons: SummerConstructor[K])
  extends Mergeable[K, V] {
  protected implicit val collector = FutureCollector.bestEffort
  protected val summer: StatefulSummer[Map[K, PromiseLink[V]]] = summerCons(new PromiseLinkSemigroup(semigroup))

  override def semigroup = store.semigroup

  // Flush the underlying buffer
  def flush: Future[Unit] =
    summer.flush.map(mergeFlush(_)) match {
      case None => Future.Unit
      case Some(mKV) => collector(mKV.values.toSeq).unit
    }

  // Return the value before, like a merge
  private def mergeFlush(toMerge: Map[K, PromiseLink[V]]): Map[K, Future[Option[V]]] =
    // Now merge any evicted items from the buffer to below:
    store
      .multiMerge(toMerge.mapValues(_.value))
      .map { case (k, foptV) =>
        val prom = toMerge(k)
        foptV.respond { prom.completeIfEmpty(_) }
        k -> foptV
      }

  override def multiMerge[K1 <: K](kvs: Map[K1, V]) = {
    val links: Map[K, PromiseLink[V]] = kvs.map { case (k1, v) => k1 -> PromiseLink(v) }(breakOut) // no lazy
    summer.put(links).foreach(mergeFlush(_))
    kvs.map { case (k, _) => k -> links(k).promise }
  }

  override def close(t: Time) = store.close(t)
}
/** A MergeableStore that does the same buffering as BufferingMergeable, but
 * flushes on put/get.
 */
class BufferingStore[K, V](store: MergeableStore[K, V], summerCons: SummerConstructor[K])
  extends BufferingMergeable[K, V](store, summerCons) with MergeableStore[K, V] {

  // Assumes m has k, which is true by construction below
  private def wait[K1<:K, W](k: K1, m: Future[Map[K1, Future[W]]]): Future[W] = m.flatMap { _.apply(k) }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val allGets = flush.map(_ => store.multiGet(ks))
    ks.map { k => k -> wait(k, allGets) }(breakOut)
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = {
    val allPuts = flush.map(_ => store.multiPut(kvs))
    kvs.map { case (k, _) => k -> wait(k, allPuts) }
  }
}

