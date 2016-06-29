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

import com.twitter.util.{Future, Await}

import org.scalacheck.Properties
import org.scalacheck.Prop._

object SearchingReadableStoreProperties extends Properties("SearchingReadableStore") {
  /**
    * The SearchingReadableStore returned by ReadableStore.find always
    * looks at substores from left to right. This test checks that the
    * store keeps searching until it finds its first non-None value.
    */
  property("ReadableStore.find works as expected") =
    forAll { (m1: Map[String, Int], m2: Map[String, Int]) =>
      val store =
        ReadableStore.find(Seq(ReadableStore.fromMap(m1), ReadableStore.fromMap(m2)))(_.isDefined)
      val leftGet = store.multiGet(m1.keySet).mapValues(Await.result(_))
      val rightGet = store.multiGet(m2.keySet).mapValues(Await.result(_))
      leftGet == m1.mapValues(Some(_)) && rightGet.forall { case (k, v) => v == (m2 ++ m1).get(k) }
    }

  /**
   * Creates a ReadableStore that delegates to underlying store and
   * records accesses.
   */
  def accessRecordingStore[K, V](underlying: ReadableStore[K, V]) =
    new ReadableStore[K, V] {
      var accesses = 0

      override def get(k: K): Future[Option[V]] = {
        this.synchronized { accesses += 1 }
        underlying.get(k)
      }

      override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
        this.synchronized { accesses += 1 }
        underlying.multiGet(ks)
      }
    }

  property("ReadableStore.find does not call the second store if value found in first") =
    forAll { (m: Map[String, Int]) =>
      val aRecStore = accessRecordingStore(ReadableStore.fromMap(Map.empty[String, Int]))
      val store = ReadableStore.find(Seq(ReadableStore.fromMap(m), aRecStore))(_.isDefined)
      val leftGet = store.multiGet(m.keySet).mapValues(Await.result(_))
      aRecStore.accesses == 0
    }
}
