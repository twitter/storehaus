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

import com.twitter.util.Await
import com.twitter.storehaus.ReadableStore
import org.scalacheck.{Prop, Arbitrary, Properties}
import org.scalacheck.Prop._

object ReadableStoreProperties extends Properties("ReadableStoreAlgebra") {
    /**
    * get returns none when not in either store
    */
  def getLaws[K: Arbitrary, V1: Arbitrary, V2: Arbitrary](
      fnA: Map[K, V1] => ReadableStore[K, V1], fnB: Map[K, V2] => ReadableStore[K, V2]): Prop =
    forAll { (mA: Map[K, V1], mB: Map[K, V2], others: Set[K]) =>
      val keysA = mA.keySet
      val keysB = mB.keySet
      val expanded: Set[K] = keysA ++ keysB ++ others
      val storeA = fnA(mA)
      val storeB = fnB(mB)
      val combinedStore = ReadableStoreAlgebra.both(storeA, storeB)
      expanded.forall {
        k: K =>
        val combinedRes = Await.result(combinedStore.get(k))
        (mA.get(k), mB.get(k)) match {
          case (Some(l), Some(r)) => combinedRes == Some(Left((l, r)))
          case (Some(l), None) => combinedRes == Some(Right(Left(l)))
          case (None, Some(r)) => combinedRes == Some(Right(Right(r)))
          case (None, None) => combinedRes.isEmpty
        }
      }
    }

  property("Parallel store matches normal queries") =
    getLaws[Int, String, Long](ReadableStore.fromMap, ReadableStore.fromMap)

}
