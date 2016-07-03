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

import com.twitter.bijection.Injection
import com.twitter.storehaus.testing.SelfAggregatingCloseableCleanup
import com.twitter.util.Await

import org.scalacheck.Properties

import scala.util.Try

object PivotedReadableStoreProperties extends Properties("PivotedReadableStore")
  with SelfAggregatingCloseableCleanup[PivotedReadableStore[String, String, Int, String]] {

  // (prefix, num) => "prefix/num"
  implicit object PivotInjection extends Injection[(String, Int), String] {
    def apply(pair: (String, Int)): String = pair._1 + "/" + pair._2.toString
    override def invert(s: String): Try[(String, Int)] = {
      val parts = s.split('/')
      Try((parts(0), parts(1).toInt))
    }
  }

  def getStoreTest(store: PivotedReadableStore[String, String, Int, String]): Boolean = {
    val innerStore1 = Await.result(store.get("prefix1")).get
    val innerStore2 = Await.result(store.get("prefix2")).get
    (0 until 100).toList.forall { case n =>
      Await.result(innerStore1.get(n)).get == "value1" + n.toString
      Await.result(innerStore2.get(n)).get == "value2" + n.toString
    }
  }

  def multiGetStoreTest(store: PivotedReadableStore[String, String, Int, String]): Boolean = {
    val innerStores = store.multiGet(Set("prefix1", "prefix2"))
    val innerStore1 = Await.result(innerStores.get("prefix1").get).get
    val innerStore2 = Await.result(innerStores.get("prefix2").get).get
    (0 until 100).toList.forall { case n =>
      Await.result(innerStore1.get(n)).get == "value1" + n.toString
      Await.result(innerStore2.get(n)).get == "value2" + n.toString
    }
  }

  property("PivotedReadableStore gets over MapStore") = {
    val map1 : Map[String, String] = (0 until 100).toList.map { case n =>
      (PivotInjection(("prefix1", n)), "value1" + n.toString)
    }.toMap
    val map2 : Map[String, String] = (0 until 100).toList.map { case n =>
      (PivotInjection(("prefix2", n)), "value2" + n.toString)
    }.toMap

    val store = PivotedReadableStore.fromMap[String, String, Int, String](map1 ++ map2)

    getStoreTest(store) && multiGetStoreTest(store)
  }
}
