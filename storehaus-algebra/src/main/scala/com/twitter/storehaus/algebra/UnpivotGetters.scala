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

import com.twitter.bijection.Pivot
import com.twitter.util.Future

/**
 * Methods used in the various unpivot stores.
 *
 * @author Sam Ritchie
 */

object UnpivotGetters {
  def get[K, OuterK, InnerK, V](k: K)(split: K => (OuterK, InnerK))(fn: OuterK => Future[Option[Map[InnerK, V]]]) = {
    val (outerK, innerK) = split(k)
    fn(outerK).map { _.flatMap { _.get(innerK) } }
  }

  def multiGet[K, T <: K, OuterK, InnerK, V](ks: Set[K])(split: K => (OuterK, InnerK))(fn: Set[OuterK] => Map[OuterK, Future[Option[Map[InnerK, V]]]]) = {
    val pivot = Pivot.encoder[K, OuterK, InnerK](split)
    val ret: Map[OuterK, Future[Option[Map[InnerK, V]]]] = fn(pivot(ks).keySet)
    ks.map { k =>
      val (outerK, innerK) = split(k)
      k -> ret(outerK).map { optM: Option[Map[InnerK, V]] =>
        optM.flatMap { _.get(innerK) }
      }
    }.toMap
  }
}
