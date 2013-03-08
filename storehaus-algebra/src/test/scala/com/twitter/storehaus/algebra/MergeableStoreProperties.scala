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

import com.twitter.algebird.{ MapAlgebra, Monoid, SummingQueue }
import com.twitter.bijection.algebird.AlgebirdBijections._
import com.twitter.bijection.Injection
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop._
import org.scalacheck.Properties
import com.twitter.storehaus.{ Store, JMapStore, FutureOps }

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
  def baseTest[K: Arbitrary, V: Arbitrary: Monoid: Equiv](store: MergeableStore[K, V])(put: (MergeableStore[K, V], List[(K, V)]) => Unit) =
    forAll { examples: List[(K, V)] =>
      val inputMap = MapAlgebra.sumByKey(examples).mapValues { Monoid.nonZeroOption(_) }
      val preResult = FutureOps.mapCollect(store.multiGet(inputMap.keySet)).get
      val expectedResult = Monoid.plus(inputMap, preResult)
        .mapValues { _.flatMap { Monoid.nonZeroOption(_) } }

      put(store, examples) // Mutate the store

      Equiv[Map[K, Option[V]]].equiv(
        expectedResult,
        FutureOps.mapCollect(store.multiGet(expectedResult.keySet)).get
      )
    }

  def newStore[K, V: Monoid]: MergeableStore[K, V] =
    MergeableStore.fromStore(new JMapStore[K, V])

  def newConvertedStore[K,V1,V2](implicit inj: Injection[V2,V1], monoid: Monoid[V2]): MergeableStore[K,V2] = {
    val store = new JMapStore[K, V1]
    val cstore = new ConvertedStore[K,K,V1,V2](store)(identity[K])
    MergeableStore.fromStore(cstore)
  }

  def singleMergeableStoreTest[K: Arbitrary, V: Arbitrary: Monoid: Equiv](store: MergeableStore[K, V]) =
    baseTest(store) { (s, pairs) => pairs.foreach { s.merge(_) } }

  def multiMergeableStoreTest[K: Arbitrary, V: Arbitrary: Monoid: Equiv](store: MergeableStore[K, V]) =
    baseTest(store) { (s, pairs) => s.multiMerge(MapAlgebra.sumByKey(pairs)) }

  def mergeableStoreTest[K: Arbitrary, V: Arbitrary: Monoid: Equiv](store: MergeableStore[K, V]) =
    singleMergeableStoreTest(store) && multiMergeableStoreTest(store)

  property("MergeableStore from JMapStore obeys the mergeable store laws") =
    mergeableStoreTest(newStore[Int, Map[Int, Int]])

  property("MergeableStore from Converted JMapStore obeys the mergeable store laws") =
    mergeableStoreTest(newConvertedStore[Int, String, Int])

  property("Converted MergeableStore obeys the mergeable store laws") = {
    // We are using a weird monoid on Int here:
    import com.twitter.bijection.algebird.AlgebirdBijections._
    import com.twitter.bijection.Conversion.asMethod
    implicit val monoid : Monoid[Int] = implicitly[Monoid[(Short,Short)]].as[Monoid[Int]]

    mergeableStoreTest {
      new ConvertedMergeableStore[Int,Int,(Short,Short),Int](newStore[Int,(Short,Short)])(identity[Int])
    }
  }
}
