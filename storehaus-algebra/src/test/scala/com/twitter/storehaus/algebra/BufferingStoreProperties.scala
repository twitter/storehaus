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

import com.twitter.algebird.{SummingQueue, Semigroup}
import com.twitter.storehaus.{ StoreProperties, JMapStore }
import org.scalacheck.Properties

object BufferingStoreProperties extends Properties("BufferingStore") {
  import StoreProperties.storeTest
  import MergeableStoreProperties._
  import MergeableStore.enrich

  property("BufferingStore [Map[Int,String]] obeys the store properties") = storeTest {
    newStore[String, Map[Int, String]].withSummer(new SummerConstructor[String] {
      def apply[V](sg: Semigroup[V]) = {
        implicit val semi = sg
        SummingQueue[Map[String, V]](10)
      }
    })
  }
  property("BufferingStore [Map[Int,Int]] obeys the store properties") = storeTest {
    newStore[String, Map[Int, Int]].withSummer(new SummerConstructor[String] {
      def apply[V](sg: Semigroup[V]) = {
        implicit val semi = sg
        SummingQueue[Map[String, V]](10)
      }
    })
  }
}
