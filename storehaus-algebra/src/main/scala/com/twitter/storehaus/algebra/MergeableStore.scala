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

import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.bijection.ImplicitBijection
import com.twitter.storehaus.{CollectionOps, FutureCollector, FutureOps, MissingValueException, Store}
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}
import scala.language.implicitConversions

/** Main trait to represent stores that are used for aggregation */
trait MergeableStore[-K, V] extends Store[K, V] with Mergeable[K, V]

/** Some factory methods and combinators on MergeableStore */
object MergeableStore {
  implicit def enrich[K, V](store: MergeableStore[K, V]): EnrichedMergeableStore[K, V] =
    new EnrichedMergeableStore(store)

  private[this] def addOpt[V](init: Option[V], inc: V)(implicit sg: Semigroup[V]): Option[V] = init match {
    case Some(i) => Some(sg.plus(i, inc))
    case None => Some(inc)
  }

  /**
   * Use the newer function with less params below. This function just
   * calls that ignoring missingfn and collect.
   */
  @deprecated
  def multiMergeFromMultiSet[K, V](store: Store[K, V], kvs: Map[K, V],
    missingfn: (K) => Future[Option[V]] = FutureOps.missingValueFor _)
    (implicit collect: FutureCollector, sg: Semigroup[V]): Map[K, Future[Option[V]]] =
    multiMergeFromMultiSet(store, kvs)

  /**
  * Implements multiMerge functionality in terms of an underlying
  * store's multiGet and multiSet.
  */
  def multiMergeFromMultiSet[K, V](store: Store[K, V], kvs: Map[K, V])
    (implicit sg: Semigroup[V]): Map[K, Future[Option[V]]] = {
    val keySet = kvs.keySet

    // Iterator to avoid creating an intermediate map
    val mGetResult: Iterator[(K, Future[(Option[V], Option[V])])] =
      store.multiGet(keySet).iterator.map { case (k, futureOptV) =>
        val newFOptV = futureOptV.map { oldV: Option[V] =>
          val incV = kvs(k)
          val newV = addOpt(oldV, incV)
          (oldV, newV)
        }
        k -> newFOptV
      }

    val collectedMGetResult = collectWithFailures(mGetResult, kvs.size)
    val getSuccesses = collectedMGetResult.map(_._1)
    val getFailures = collectedMGetResult.map(_._2)

    val mPutResultsFut: Future[Map[K, Future[Unit]]] =
      getSuccesses.map { ss: Map[K, (Option[V], Option[V])] =>
        store.multiPut(ss.mapValues(_._2))
      }

    @inline
    def mapToOldValue(k: K, fUnit: Future[Unit]): Future[Option[V]] =
      fUnit.flatMap { _ =>
        getSuccesses.map { ss => ss(k)._1 }
      }

    @inline
    def lookupGetFailure(k: K): Future[Option[V]] =
      getFailures.flatMap { failures =>
        Future.exception(failures.get(k).getOrElse(new MissingValueException[K](k)))
      }

    /**
     *  A bit complex but it saves us an intermediate map creation. Here's the logic:
     *  If key is present in put results map successful ones to old value, failures remain.
     *  If not in put results then map to corresponding get failure.
     *  Ultimately we will have successful puts(mapped to old value), put failures and get failures.
     */
    @inline
    def keyMapFn(k: K): Future[Option[V]] = mPutResultsFut.flatMap { mPutResults =>
      mPutResults.get(k) match {
        case Some(fUnit) => mapToOldValue(k, fUnit)
        case None => lookupGetFailure(k)
      }
    }

    CollectionOps.zipWith(keySet)(keyMapFn)
  }

  /**
   * Collects keyed futures, partitioning out the failures.
   */
  private[algebra] def collectWithFailures[K, V](
    fs: Iterator[(K, Future[V])], size: Int): Future[(Map[K, V], Map[K, Throwable])] = {

    if (!fs.hasNext) {
      Future.value((Map.empty[K, V], Map.empty[K, Throwable]))
    } else {
      val results = new AtomicReferenceArray[(K, Try[V])](size)
      val countdown = new AtomicInteger(size)
      val pResult = new Promise[(Map[K, V], Map[K, Throwable])]

      @inline
      def collectResults() = {
        if (countdown.decrementAndGet() == 0) {
          var successes = Map.empty[K, V]
          var failures = Map.empty[K, Throwable]
          var ri = 0
          while (ri < size) {
            results.get(ri) match {
              case (k, Return(v)) => successes = successes + (k -> v)
              case (k, Throw(t)) =>  failures = failures + (k -> t)
            }
            ri += 1
          }
          pResult.setValue((successes, failures))
        }
      }

      var i = 0
      while(fs.hasNext) {
        val (k, fv) = fs.next
        val j = i // Need to make sure we close over a copy of i and not i itself
        fv respond {
          case Return(v) =>
            results.set(j, k -> Return(v))
            collectResults()
          case Throw(cause) =>
            results.set(j, k -> Throw(cause))
            collectResults()
        }
        i += 1
      }
      pResult
    }
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
  def fromStore[K, V](store: Store[K, V])(implicit sg: Semigroup[V],
      fc: FutureCollector): MergeableStore[K, V] =
    new MergeableStoreViaGetPut[K, V](store, fc)

  /** Create a mergeable by implementing merge with single get followed by put for each key.
   * Also forces multiGet and multiPut to use the store's default implementation of a single
   * get and put.
   * The merge is only safe if each key is owned by a single thread. Useful in certain cases
   * where multiGets and multiPuts may result in higher error rates or lower throughput.
   */
  def fromStoreNoMulti[K, V](store: Store[K, V])(implicit sg: Semigroup[V]): MergeableStore[K, V] =
    new MergeableStoreViaSingleGetPut[K, V](store)

  /** Create a mergeable by implementing merge with get followed by put.
   * Only safe if each key is owned by a single thread.
   * This deletes zeros on put, but returns zero on empty (never returns None).
   * Useful for sparse storage of counts, etc...
   */
  def fromStoreEmptyIsZero[K, V](store: Store[K, V])(implicit mon: Monoid[V],
      fc: FutureCollector): MergeableStore[K, V] =
    new MergeableMonoidStore[K, V](store, fc)

  /** Use a StatefulSummer to buffer results before calling merge.
   * Useful when merging to a remote store, of if you have some very hot keys
   */
  def withSummer[K, V](
    store: MergeableStore[K, V])(
    summerCons: SummerConstructor[K]
  ): MergeableStore[K, V] = new BufferingStore(store, summerCons)

  /** Convert the key and value type of this mergeable.
   * Note this just bijects the Monoid, so the underlying monoid action is unchanged. For instance
   * if you did a Bijection from Long to (Int,Int), the underlying monoid would still be long,
   * not the default (Int,Int) monoid which works differently. Use of this probably requires
   * careful design.
   */
  def convert[K1, K2, V1, V2](store: MergeableStore[K1, V1])(kfn: K2 => K1)
    (implicit bij: ImplicitBijection[V2, V1]): MergeableStore[K2, V2] =
    new ConvertedMergeableStore(store)(kfn)
}
