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

import com.twitter.storehaus.{ StoreProperties, JMapStore }
import org.scalacheck.Properties

object UnpivotedStoreProperties extends Properties("UnpivotedStore") {
  import StoreProperties.storeTest
  import MergeableStoreProperties.{ newStore, mergeableStoreTest }
  import MergeableStoreAlgebra._

  property("UnpivotedStore obeys the store properties") = storeTest {
    val mergeableStore = MergeableStore.fromStore(new JMapStore[String, Map[Int, String]])
    new UnpivotedStore[(String, Int), String, Int, String](mergeableStore)(identity)
  }

  property("UnpivotedMergeableStore from JMapStore obeys the store laws") =
    mergeableStoreTest {
      newStore[String, Map[Int, Int]].unpivot[(String, Int), Int, Int](identity)
    }
}
