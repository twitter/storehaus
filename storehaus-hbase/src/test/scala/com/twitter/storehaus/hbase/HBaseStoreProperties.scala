/*
 * Copyright 2014 Twitter Inc.
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

package com.twitter.storehaus.hbase

import com.twitter.storehaus.Store
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.Await
import org.scalacheck.{Prop, Arbitrary, Gen, Properties}
import org.scalacheck.Prop._

/**
 * @author Mansur Ashraf
 * @since 9/8/13
 */
object HBaseStoreProperties extends Properties("HBaseStore")
  with DefaultHBaseCluster[Store[String, String]] {

  def putAndGetTest[K: Arbitrary, V: Arbitrary : Equiv](
      store: Store[K, V], pairs: Gen[List[(K, Option[V])]]): Prop =
    forAll(pairs) { examples =>
      examples.forall {
        case (k, v) =>
          Await.result(store.put((k, v)))
          val found = Await.result(store.get(k))
          Equiv[Option[V]].equiv(found, v)
      }
    }

  val closeable =
    HBaseStringStore(quorumNames, table, columnFamily, column, createTable, pool, conf, 4)
  property("HBaseStore[String, String]") =
    putAndGetTest(closeable, NonEmpty.Pairing.alphaStrs())
}
