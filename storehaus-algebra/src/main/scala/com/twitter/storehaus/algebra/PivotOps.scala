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

import com.twitter.algebird.Monoid
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.bijection.Pivot
import com.twitter.util.Future
import com.twitter.storehaus.{ FutureCollector, Store }

/**
 * Methods used in the various unpivot stores.
 *
 * @author Sam Ritchie
 */

object PivotOps {
  def get[K, OuterK, InnerK, V](k: K)(split: K => (OuterK, InnerK))(fn: OuterK => Future[Option[Map[InnerK, V]]]) = {
    val (outerK, innerK) = split(k)
    fn(outerK).map { _.flatMap { _.get(innerK) } }
  }

  def multiGet[K, OuterK, InnerK, V](ks: Set[K])(split: K => (OuterK, InnerK))(fn: Set[OuterK] => Map[OuterK, Future[Option[Map[InnerK, V]]]]) = {
    val pivot = Pivot.encoder[K, OuterK, InnerK](split)
    val ret: Map[OuterK, Future[Option[Map[InnerK, V]]]] = fn(pivot(ks).keySet)
    ks.map { k =>
      val (outerK, innerK) = split(k)
      k -> ret(outerK).map { optM: Option[Map[InnerK, V]] =>
        optM.flatMap { _.get(innerK) }
      }
    }.toMap
  }
    /**
    * Queries the underlying store with a multiGet and transforms the
    * underlying map by filtering out all (innerK -> v) pairs that a)
    * contain None as the value or 2) in combination with the paired
    * outerK don't pass the input filter.
    */
  def multiGetFiltered[OuterK, InnerK, V](store: Store[OuterK, Map[InnerK, V]], ks: Set[OuterK])
    (pred: (OuterK, InnerK) => Boolean)
      : Map[OuterK, Future[Option[List[(InnerK, V)]]]] =
    store.multiGet(ks)
      .map { case (outerK, futureOptV) =>
        outerK -> futureOptV.map { optV =>
          optV.map { _.filterKeys { pred(outerK, _) }.toList }
            .filter { _.isEmpty }
        }
      }

  /**
    * For each value, filters out InnerK entries with a value of None.
    */
  def collectPivoted[K, OuterK, InnerK, V](pivoted: Map[OuterK, Map[InnerK, Option[V]]])
      : Map[OuterK, Future[Option[List[(InnerK, V)]]]] =
    pivoted.mapValues { m =>
      Future.value {
        Some(m.collect { case (innerK, Some(v)) => innerK -> v }.toList)
          .filter { _.isEmpty }
      }
    }

  type InnerPair[OuterK, InnerK, V] = (OuterK, Option[Map[InnerK,V]])

  def multiPut[K, K1 <: K, OuterK, InnerK, V](
    store: Store[OuterK, Map[InnerK, V]],
    kvs: Map[K1, Option[V]]
  )(
    split: K => (OuterK, InnerK)
  )(implicit collect: FutureCollector[InnerPair[OuterK, InnerK, V]]): Map[K1, Future[Unit]] = {
    val pivoted = MapPivotEncoder[K1, OuterK, InnerK, Option[V]](kvs)(split)

    // Input data merged with all relevant data from the underlying
    // store.
    val mergedResult: Map[OuterK, Future[Option[Map[InnerK, V]]]] =
      Monoid.plus(
        multiGetFiltered(store, pivoted.keySet) { case (outerK, innerK) =>
            val pivotedInnerM = pivoted(outerK)
            pivotedInnerM.contains(innerK) && !pivotedInnerM(innerK).isDefined
        },
        collectPivoted(pivoted)
      ).mapValues { _.map { _.map { _.toMap } } }

    // Result of a multiPut of all affected pairs in the underlying
    // store.
    val submitted: Future[Map[OuterK, Future[Unit]]] =
      Store.mapCollect(mergedResult)(collect).map { store.multiPut(_) }

    // The final flatMap returns a map of K to the future responsible
    // for writing K's value into the underlying store. Due to
    // packing, many Ks will reference the same Future[Unit].
    kvs.flatMap {
      case (k, _) =>
        val (outerK, _) = split(k)
        (1 to pivoted(outerK).size).map { _ =>
          k -> submitted.flatMap { _.apply(outerK) }
        }
    }.toMap
  }
}
