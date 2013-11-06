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

import com.twitter.storehaus._
import com.twitter.storehaus.algebra._

object ReportingStoreProperties extends Properties("ReportingStore") {
  def newStore[K, V] = new JMapStore[K, V]

  property("Put Some/None count matches") = forAll { (inserts: Map[Int, Option[Int]]) =>
        var putSomeCount = 0
        var putNoneCount = 0
        val reporter = new StoreReporter[Int, Int] {
          override def tracePut(kv: (Int, Option[Int]), request: Future[Unit]) = {
            Future {
                kv._2 match {
                case Some(_) => putSomeCount += 1
                case None => putNoneCount += 1
                }
            }.unit
          }
        }
        val baseStore = newStore[Int, Int]
        val wrappedStore = ReportingStore[Int, Int](baseStore, reporter)

        inserts.foreach{ i =>
          wrappedStore.put(i._1, i._2)
        }
        inserts.map(_._2).collect{case Some(b) => b}.size == putSomeCount &&
          inserts.map(_._2).collect{case None => 1}.size == putNoneCount
  }

  property("MultiPut Some/None count matches") = forAll { (inserts: Map[Int, Option[Int]]) =>
        var multiPutSomeCount = 0
        var multiPutNoneCount = 0
        val reporter = new StoreReporter[Int, Int] {
          override def traceMultiPut[K1 <: Int](kvs: Map[K1, Option[Int]], request: Map[K1, Future[Unit]]): Map[K1, Future[Unit]] = {
            kvs.mapValues {v =>
              Future {
                v match {
                case Some(_) => multiPutSomeCount += 1
                case None => multiPutNoneCount += 1
                }
              }.unit
            }
          }
        }
        val baseStore = newStore[Int, Int]
        val wrappedStore = ReportingStore[Int, Int](baseStore, reporter)


        wrappedStore.multiPut(inserts)

        inserts.map(_._2).collect{case Some(b) => b}.size == multiPutSomeCount &&
          inserts.map(_._2).collect{case None => 1}.size == multiPutNoneCount
  }
}
