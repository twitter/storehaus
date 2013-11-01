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

import org.scalacheck.Properties
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

object ShardedStoreProperties extends Properties("ShardedStore") {
  import StoreProperties.storeTest

  def moddiv(v: Int, by: Int): (Int, Int) = {
    val cmod = v % by
    val mod = if(cmod < 0) cmod + by else cmod
    (mod, v/by)
  }

  implicit val strArb: Arbitrary[String] = Arbitrary { Gen.alphaStr }
  property("ShardedStore test") = {
    val SHARDS = 10
    implicit val arbpair: Arbitrary[(Int,Int)] = Arbitrary { Arbitrary.arbitrary[Int].map { moddiv(_, SHARDS) } }
    storeTest {
      ShardedStore.fromMap((0 until SHARDS).map { _ -> new JMapStore[Int, String] }.toMap)
    }
  }
}
