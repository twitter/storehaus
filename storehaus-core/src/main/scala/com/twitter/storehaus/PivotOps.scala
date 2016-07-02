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

/**
 * Methods used in the various unpivot stores.
 *
 * @author Sam Ritchie
 */

object PivotOps {
  /**
    * Queries the underlying store with a multiGet and transforms the
    * underlying map by filtering out all (innerK -> v) pairs that a)
    * contain None as the value or 2) in combination with the paired
    * outerK don't pass the input filter.
    */
  def multiGetFiltered[OuterK, InnerK, V](
    store: ReadableStore[OuterK, Map[InnerK, V]], ks: Set[OuterK])
    (pred: (OuterK, InnerK) => Boolean)
      : Map[OuterK, Future[Option[List[(InnerK, V)]]]] =
    store.multiGet(ks)
      .map { case (outerK, futureOptV) =>
        outerK -> futureOptV.map { optV =>
          optV.map { _.filterKeys { pred(outerK, _) }.toList }
            .filter(_.nonEmpty)
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
          .filter(_.nonEmpty)
      }
    }

  type InnerPair[OuterK, InnerK, V] = (OuterK, Option[Map[InnerK, V]])

  /**
    * Really belongs in Algebird, but recoding this explicitly keeps the dependency out.
    */
  private def plusM[K, V](
    l: Map[K, Future[Option[List[V]]]],
    r: Map[K, Future[Option[List[V]]]]
  ): Map[K, Future[Option[List[V]]]] =
    (r /: l) { case (m, (k, futureOptV)) =>
        val newV = for {
          leftOptV <- futureOptV
          rightOptV <- m.getOrElse(k, Future.None)
        } yield (leftOptV, rightOptV) match {
          case (None, None) => None
          case (None, Some(v)) => Some(v)
          case (Some(v), None) => Some(v)
          case (Some(left), Some(right)) => Some(left ++ right)
        }
        m + (k -> newV)
    }

  def multiPut[K, K1 <: K, OuterK, InnerK, V](
    store: Store[OuterK, Map[InnerK, V]], kvs: Map[K1, Option[V]])
    (split: K => (OuterK, InnerK))
    (implicit collect: FutureCollector): Map[K1, Future[Unit]] = {
    val pivoted = CollectionOps.pivotMap[K1, OuterK, InnerK, Option[V]](kvs)(split)

    // Input data merged with all relevant data from the underlying
    // store.
    val mergedResult: Map[OuterK, Future[Option[Map[InnerK, V]]]] =
      plusM(
        multiGetFiltered(store, pivoted.keySet) { case (outerK, innerK) =>
            !pivoted(outerK).contains(innerK)
        },
        collectPivoted(pivoted)
      ).mapValues { _.map { _.map { _.toMap } } }

    // Result of a multiPut of all affected pairs in the underlying
    // store.
    val submitted: Future[Map[OuterK, Future[Unit]]] =
      FutureOps.mapCollect(mergedResult)(collect).map(store.multiPut)

    // The final flatMap returns a map of K to the future responsible
    // for writing K's value into the underlying store. Due to
    // packing, many Ks will reference the same Future[Unit].
    kvs.flatMap {
      case (k, _) =>
        val (outerK, _) = split(k)
        (1 to pivoted(outerK).size).map { _ =>
          k -> submitted.flatMap { _.apply(outerK) }
        }
    }
  }
}
