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

/**
 * PivotEncoder to Map[InnerK, V] (vs Iterable[(InnerK, V)], as
 * bijection.Pivot would provide).
 *
 * @author Sam Ritchie
 */

object MapPivotEncoder {
  def apply[K, OuterK, InnerK, V](pairs: Map[K, V])(split: K => (OuterK, InnerK)): Map[OuterK, Map[InnerK, V]] =
    pairs.toList.map {
      case (k, v) =>
        val (outerK, innerK) = split(k)
        (outerK -> (innerK -> v))
    }
      .groupBy { _._1 }
      .mapValues { _.map { _._2 }.toMap }
}
