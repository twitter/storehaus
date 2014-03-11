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
import com.twitter.storehaus.{ReadableStore, ReadableStoreProxy}
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object ReportingReadableStoreProperties extends Properties("ReportingReadableStore") {
    /**
    * get returns none when not in either store
    */

   class DummyReporter[K, V](val self: ReadableStore[K, V]) extends ReadableStoreProxy[K, V] with ReadableStoreReporter[ReadableStore[K, V], K, V] {
    def traceMultiGet[K1 <: K](ks: Set[K1], request: Map[K1, Future[Option[V]]]) = request.mapValues(_.unit)
    def traceGet(k: K, request: Future[Option[V]]) = request.unit
  }

  def buildStoreRunQueries[K, V](mA: Map[K, V], others: Set[K], builder: (ReadableStore[K, V]) => ReadableStore[K, V]) = {
    val baseStore = ReadableStore.fromMap(mA)
    val wrappedStore = builder(baseStore)
    val expanded: Set[K] = (mA.keySet ++ others)
    // We use call to list, or it keeps the results of the map as a set and we loose data
    expanded.toList.map{k: K => (mA.get(k), Await.result(wrappedStore.get(k)))}
  }

  def buildStoreRunMultiGetQueries[K, V](mA: Map[K, V], others: Set[K], builder: (ReadableStore[K, V]) => ReadableStore[K, V]) = {
    val baseStore = ReadableStore.fromMap(mA)
    val wrappedStore = builder(baseStore)
    val expanded: Set[K] = (mA.keySet ++ others)
    // We use call to list, or it keeps the results of the map as a set and we loose data
    (expanded.map{k => (k, mA.get(k))}.toMap,
      wrappedStore.multiGet(expanded).map{case (k, futureV) => (k, Await.result(futureV))})
  }

  property("Stats store matches raw get for all queries") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
        def reporter(store: ReadableStore[Int, String]) = new DummyReporter[Int, String](store)
        val queryResults = buildStoreRunQueries(mA, others, reporter)
        queryResults.forall{case (a, b) => a == b}
  }

  property("Present/Absent count matches") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
        var presentCount = 0
        var absentCount = 0
        def reporter(store: ReadableStore[Int, String]) = new DummyReporter[Int, String](store) {
          override def traceGet(k: Int, request: Future[Option[String]]) = {
            request.map{ optV =>
              optV match {
                case Some(_) => presentCount += 1
                case None => absentCount += 1
              }
            }
          }
        }
        val queryResults = buildStoreRunQueries(mA, others, reporter)
        val wrappedResults = queryResults.map(_._2)
        val referenceResults = queryResults.map(_._2)
        wrappedResults.collect{case Some(b) => b}.size == presentCount &&
          wrappedResults.collect{case None => 1}.size == absentCount
  }

  property("Stats store matches raw get for multiget all queries") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
        def reporter(store: ReadableStore[Int, String]) = new DummyReporter[Int, String](store)
        val (mapRes, storeResults) = buildStoreRunMultiGetQueries(mA, others, reporter)
        mapRes.size == storeResults.size &&
          mapRes.keySet.forall(k => mapRes.get(k) == storeResults.get(k))
  }

  property("Present/Absent count matches in multiget") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
        var presentCount = 0
        var absentCount = 0

        def reporter(store: ReadableStore[Int, String]) = new DummyReporter[Int, String](store) {
          override def traceMultiGet[K1 <: Int](ks: Set[K1], request: Map[K1, Future[Option[String]]]) = {
            request.mapValues{fOptV =>
              fOptV.map {optV =>
                optV match {
                  case Some(_) => presentCount += 1
                  case None => absentCount += 1
                }
              }
            }
          }
        }

        val (_, storeResults) = buildStoreRunMultiGetQueries(mA, others, reporter)
        storeResults.values.collect{case Some(b) => b}.size == presentCount &&
          storeResults.values.collect{case None => 1}.size == absentCount
  }
}
