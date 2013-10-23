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

import com.twitter.algebird.{ Semigroup, Monoid, StatefulSummer }
import com.twitter.bijection.ImplicitBijection
import com.twitter.storehaus.{ CollectionOps, FutureCollector, Store }
import com.twitter.util.Future

/** Main trait to represent stores that are used for aggregation */
trait MergeableStore[-K, V] extends Store[K, V] {
  /** The semigroup equivalent to the merge operation of this store */
  def semigroup: Semigroup[V]
  /** Returns the value JUST BEFORE the merge. If it is empty, it is like a zero.
   * the key should hold:
   * val (k,v) = kv
   * result = get(k)
   * key is set to: result.map(Semigroup.plus(_, Some(v)).getOrElse(v) after this.
   */
  def merge(kv: (K, V)): Future[Option[V]] = multiMerge(Map(kv)).apply(kv._1)
  /** merge a set of keys. */
  def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = kvs.map { kv => (kv._1, merge(kv)) }
}

/** Some factory methods and combinators on MergeableStore */
object MergeableStore {
  implicit def enrich[K, V](store: MergeableStore[K, V]): EnrichedMergeableStore[K, V] =
    new EnrichedMergeableStore(store)

    /**
    * Implements multiMerge functionality in terms of an underlying
    * store's multiGet and multiSet.
    */
  def multiMergeFromMultiSet[K, V](store: Store[K, V], kvs: Map[K, V])
    (implicit collect: FutureCollector[(K, (Option[V], Option[V]))], sg: Semigroup[V]): Map[K, Future[Option[V]]] = {
    val keySet = kvs.keySet
    val collected: Future[Map[K, Future[Option[V]]]] =
      collect {
        store.multiGet(keySet).view.map {
          case (k, futureOptV) =>
            futureOptV.map { init =>
              val incV = kvs(k)
              val resV = init.map(Semigroup.plus(_, incV)).orElse(Some(incV))
              k -> (init, resV)
            }
        }.toSeq
      }.map { pairs: Seq[(K, (Option[V], Option[V]))] =>
        val pairMap = pairs.toMap
        store.multiPut(pairMap.mapValues(_._2))
          .map { case (k, funit) => (k, funit.map { _ => pairMap(k)._1 }) }
      }
    CollectionOps.zipWith(keySet) { k => collected.flatMap { _.apply(k) } }
  }

  /** unpivot or uncurry this MergeableStore
   * TODO: not clear is correct. It is injecting whatever Semigroup is present at call time
   * not the actual Semigroup being used by the underlying store. I guess we need to unpivot
   * the Semigroup as well (and might not even be well defined).
   * If the Semigroup is the usual mapMonoid, everything is fine.
   */
  def unpivot[K, OuterK, InnerK, V: Semigroup](store: MergeableStore[OuterK, Map[InnerK, V]])
    (split: K => (OuterK, InnerK)): MergeableStore[K, V] =
    new UnpivotedMergeableStore(store)(split)

  /** Create a mergeable by implementing merge with get followed by put.
   * Only safe if each key is owned by a single thread.
   */
  def fromStore[K,V](store: Store[K,V])(implicit sg: Semigroup[V],
      fc: FutureCollector[(K, Option[V])]): MergeableStore[K,V] =
    new MergeableStoreViaGetPut[K, V](store, fc)

  /** Create a mergeable by implementing merge with get followed by put.
   * Only safe if each key is owned by a single thread.
   * This deletes zeros on put, but returns zero on empty (never returns None).
   * Useful for sparse storage of counts, etc...
   */
  def fromStoreEmptyIsZero[K,V](store: Store[K,V])(implicit mon: Monoid[V],
      fc: FutureCollector[(K, Option[V])]): MergeableStore[K,V] =
    new MergeableMonoidStore[K, V](store, fc)

  /** Use a StatefulSummer to buffer results before calling merge.
   * Useful when merging to a remote store, of if you have some very hot keys
   */
  def withSummer[K, V](store: MergeableStore[K, V])(summerCons: SummerConstructor[K]): MergeableStore[K, V] =
    new BufferingStore(store, summerCons)

  /** Convert the key and value type of this mergeable.
   * Note this just bijects the Monoid, so the underlying monoid action is unchanged. For instance
   * if you did a Bijection from Long to (Int,Int), the underlying monoid would still be long, not the
   * default (Int,Int) monoid which works differently. Use of this probably requires careful design.
   */
  def convert[K1, K2, V1, V2](store: MergeableStore[K1, V1])(kfn: K2 => K1)
    (implicit bij: ImplicitBijection[V2, V1]): MergeableStore[K2, V2] =
    new ConvertedMergeableStore(store)(kfn)
}
