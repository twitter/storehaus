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

import com.twitter.util.{Future, Time}

/**
 * ReadableStore enrichment which presents a ReadableStore[K, V] over
 * top of a packed ReadableStore[OuterK, Map[InnerK, V]].
 *
 * @author Sam Ritchie
 */

class UnpivotedReadableStore[-K, OuterK, InnerK, +V](store: ReadableStore[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK))
  extends ReadableStore[K, V] {

  override def get(k: K) = {
    val (outerK, innerK) = split(k)
    store.get(outerK).map { _.flatMap { _.get(innerK) } }
  }

  private def pivot(pairs: Iterable[K]): Map[OuterK, Iterable[InnerK]] =
    pairs.map { k =>
      val (k1, k2) = split(k)
      (k1 -> List(k2))
    }.groupBy { _._1 }
      .mapValues { _.map { case (_, k2s) => k2s }.flatten }

  override def multiGet[T <: K](ks: Set[T]): Map[T, Future[Option[V]]] = {
    val ret: Map[OuterK, Future[Option[Map[InnerK, V]]]] = store.multiGet(pivot(ks).keySet)
    ks.map { k =>
      val (outerK, innerK) = split(k)
      k -> ret(outerK).map { optM: Option[Map[InnerK, V]] =>
        optM.flatMap { _.get(innerK) }
      }
    }.toMap
  }
  override def close(time: Time) = store.close(time)
}
