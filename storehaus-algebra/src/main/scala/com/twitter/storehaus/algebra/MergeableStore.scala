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

import com.twitter.algebird.{Monoid, MapMonoid, Semigroup, MapAlgebra}
import com.twitter.storehaus._

import Store.mapCollect

import com.twitter.algebird.util.UtilAlgebras._

import com.twitter.util.{Future}

// Needed for the monoid on Future[V]
import com.twitter.algebird.util.UtilAlgebras._

/** None is indistinguishable from monoid.zero
 */
trait MergeableStore[-K, V] extends Store[K, V] {
  def monoid[V]: Monoid[V]
  /** sets to monoid.plus(get(kv._1).get.getOrElse(monoid.zero), kv._2)
   * but maybe more efficient implementations
   */
  def merge(kv: (K, V)): Future[Unit] =
    for(vOpt <- get(kv._1);
        oldV = vOpt.getOrElse(monoid.zero);
        newV = monoid.plus(oldV, kv._2);
        finalUnit <- put((kv._1, monoid.nonZeroOption(newV)))) yield finalUnit

  def multiMerge[K1<:K](kvs: Map[K1,V]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, merge(kv)) }
}
