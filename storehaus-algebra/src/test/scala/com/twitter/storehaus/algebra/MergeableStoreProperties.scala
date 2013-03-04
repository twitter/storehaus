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

import com.twitter.algebird.{ MapAlgebra, Monoid }
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import org.scalacheck.Properties
import com.twitter.storehaus.{ Store, JMapStore }

import StoreAlgebra._

object MergeableStoreProperties extends Properties("MergeableStore") {
  def rightContainsLeft[K,V: Equiv](l: Map[K, V], r: Map[K, V]): Boolean =
    l.foldLeft(true) { (acc, pair) =>
      acc && r.get(pair._1).map { Equiv[V].equiv(_, pair._2) }.getOrElse(true)
    }

  // The optional equiv included in scalacheck doesn't pull in an
  // implicit Equiv[T].
  implicit def optEquiv[T: Equiv]: Equiv[Option[T]] =
    Equiv.fromFunction { (opt1, opt2) =>
      (opt1, opt2) match {
        case (Some(l), Some(r)) => Equiv[T].equiv(l, r)
        case (None, None) => true
        case (Some(_), None) => false
        case (None, Some(_)) => false
      }
    }

  implicit def mapEquiv[K,V: Monoid: Equiv]: Equiv[Map[K, V]] = {
    Equiv.fromFunction { (m1, m2) =>
      val cleanM1 = MapAlgebra.removeZeros(m1)
      val cleanM2 = MapAlgebra.removeZeros(m2)
      rightContainsLeft(cleanM1, cleanM2) && rightContainsLeft(cleanM2, cleanM1)
    }
  }

  // Adds a bunch of items and removes them and sees if they are absent
  def mergeableStoreTest[K: Arbitrary, V: Arbitrary: Monoid: Equiv](store: MergeableStore[K, V])(put: (MergeableStore[K, V], List[(K, V)]) => Unit) =
    forAll { examples: List[(K, V)] =>
      val inputMap = MapAlgebra.sumByKey(examples).mapValues { Monoid.nonZeroOption(_) }
      val preResult = Store.mapCollect(store.multiGet(inputMap.keySet)).get
      val expectedResult = Monoid.plus(inputMap, preResult)
        .mapValues { _.flatMap { Monoid.nonZeroOption(_) } }

      put(store, examples) // Mutate the store

      Equiv[Map[K, Option[V]]].equiv(
        expectedResult,
        Store.mapCollect(store.multiGet(expectedResult.keySet)).get
      )
    }

  def newStore[K, V: Monoid]: MergeableStore[K, V] =
    MergeableStore.fromStore(new JMapStore[K, V])

  property("MergeableStore from JMapStore works with single merge") =
    mergeableStoreTest(newStore[Int, Map[Int, Int]]) { (s, pairs) =>
      pairs.foreach { s.merge(_).get }
    }

  property("MergeableStore from JMapStore works with multiMerge") =
    mergeableStoreTest(newStore[Int, Int]) { (s, pairs) =>
      val input = MapAlgebra.sumByKey(pairs)
      s.multiMerge(input).foreach { case (_, v) => v.get }
    }

  property("UnpivotedMergeableStore from JMapStore works with single merge") = {
    val store = newStore[String, Map[Int, Int]].unpivot[(String, Int), Int, Int](identity)
    mergeableStoreTest(store) { (s, pairs) =>
      pairs.foreach { s.merge(_).get }
    }
  }

  property("UnpivotedMergeableStore from JMapStore works with multiMerge") = {
    val unpivoted = newStore[Int, Map[Int, Int]].unpivot[(Int, Int), Int, Int](identity)
    mergeableStoreTest(unpivoted) { (s, pairs) =>
      val input = MapAlgebra.sumByKey(pairs)
      s.multiMerge(input).foreach { case (_, v) => v.get }
    }
  }
}
