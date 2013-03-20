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

/** Helpful transformations on maps and collections.
 * These are combinators or transformers on collections that should probably be somewhere in the
 * scala collection API, but are not.
 */

object CollectionOps {
  /**
   * PivotEncoder to Map[InnerK, V] (vs Iterable[(InnerK, V)], as
   * com.twitter.bijection.Pivot would provide).
   */
  def pivotMap[K, OuterK, InnerK, V](pairs: Map[K, V])(split: K => (OuterK, InnerK)): Map[OuterK, Map[InnerK, V]] =
    pairs.toList.map {
      case (k, v) =>
        val (outerK, innerK) = split(k)
        (outerK -> (innerK -> v))
    }
      .groupBy { _._1 }
      .mapValues { _.map { _._2 }.toMap }

  /** create a Map from a Set of keys and a lookup function */
  def zipWith[K, V](keys: Set[K])(lookup: K => V): Map[K, V] =
    keys.foldLeft(Map.empty[K, V]) { (m, k) => m + (k -> lookup(k)) }

  /** from a Seq of Maps produce a Map with values being Seqs.
   * Note that it is impossible to reverse this function because
   * we lose track of which V came from which Map.
   * Often used with a [[com.twitter.storehaus.FutureCollector]] on the inner
   * value.
   */
  def combineMaps[K, V](m: Seq[Map[K, V]]): Map[K, Seq[V]] =
    m.foldLeft(Map[K, List[V]]()) { (oldM, mkv) =>
      mkv.foldLeft(oldM) { (seqm, kv) =>
        seqm + (kv._1 -> (kv._2 :: seqm.getOrElse(kv._1, Nil)))
      }
    }.mapValues { _.reverse }
}
