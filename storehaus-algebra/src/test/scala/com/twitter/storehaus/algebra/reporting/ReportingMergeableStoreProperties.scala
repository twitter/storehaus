
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

package com.twitter.storehaus.algebra.reporting

import com.twitter.util.{ Await, Future }
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

import com.twitter.storehaus.algebra._

object ReportingMergeableStoreProperties extends Properties("ReportingMergeableStore") {
  import MergeableStoreProperties.{mergeableStoreTest, newStore}



  class DummyReporter[K, V](val self: Mergeable[K, V]) extends MergeableProxy[K, V] with MergeableReporter[K, V] {
    def traceMerge(kv: (K, V), request: Future[Option[V]]) = request.unit
    def traceMultiMerge[K1 <: K](kvs: Map[K1, V], request: Map[K1, Future[Option[V]]]) = request.mapValues(_.unit)
  }

  property("Mergable stat store obeys the mergeable store proporites") =
    mergeableStoreTest {
      val store = newStore[Int, Int]
      new MergeableStoreProxy[Int, Int] with MergeableReporter[Int, Int] {
        val self = store
        def traceMerge(kv: (Int, Int), request: Future[Option[Int]]) = request.unit
        def traceMultiMerge[K1 <: Int](kvs: Map[K1, Int], request: Map[K1, Future[Option[Int]]]) = request.mapValues(_.unit)
      }
    }

  property("merge Some/None count matches") = forAll { (base: Map[Int, Int], merge: Map[Int, Int]) =>
        var mergeWithSomeCount = 0
        var mergeWithNoneCount = 0
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        baseStore.multiMerge(base)

        val wrappedStore = new DummyReporter[Int, Int](baseStore) {
          override def traceMerge(kv: (Int, Int), request: Future[Option[Int]]) = {
            request.map { optV =>
              optV match {
                case Some(_) => mergeWithSomeCount += 1
                case None => mergeWithNoneCount += 1
                }
            }.unit
          }
        }

        merge.map(kv => wrappedStore.merge((kv._1, kv._2)))

        val existsBeforeList = merge.keySet.toList.map(k => base.get(k))

        existsBeforeList.collect{case Some(_) => 1}.size == mergeWithSomeCount &&
          existsBeforeList.collect{case None => 1}.size == mergeWithNoneCount
  }

  property("multiMerge Some/None count matches") = forAll { (base: Map[Int, Int], merge: Map[Int, Int]) =>
        var mergeWithSomeCount = 0
        var mergeWithNoneCount = 0
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        baseStore.multiMerge(base)

        val wrappedStore = new DummyReporter[Int, Int](baseStore) {
          override def traceMultiMerge[K1 <: Int](kvs: Map[K1, Int], request: Map[K1, Future[Option[Int]]]) = {
            request.mapValues{optV =>
              optV.map{ v =>
                v match {
                  case Some(_) => mergeWithSomeCount += 1
                  case None => mergeWithNoneCount += 1
                }
              }.unit
            }
          }
        }

        wrappedStore.multiMerge(merge)

        val existsBeforeList = merge.keySet.toList.map(k => base.get(k))

        existsBeforeList.collect{case Some(_) => 1}.size == mergeWithSomeCount &&
          existsBeforeList.collect{case None => 1}.size == mergeWithNoneCount
  }
}
