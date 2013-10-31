
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

import com.twitter.util.{ Await, Future }
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

object MergableStatStoreProperties extends Properties("ReadableStatStore") {
  import MergeableStoreProperties.{mergeableStoreTest, newStore}

  property("Mergable stat store obeys the mergeable store proporites") =
    mergeableStoreTest {
      new MergableStatStore[Int,Int](newStore[Int, Int], new StatReporter[Int, Int] {})
    }

  property("Put Some/None count matches") = forAll { (inserts: Map[Int, Option[Int]]) =>
        var putSomeCount = 0
        var putNoneCount = 0
        val reporter = new StatReporter[Int, Int] {
          override def putSome:Unit = putSomeCount += 1
          override def putNone:Unit = putNoneCount += 1
        }
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        val wrappedStore = MergableStatStore[Int, Int](baseStore, reporter)


        inserts.foreach{ i =>
          wrappedStore.put(i._1, i._2)
        }
        inserts.map(_._2).collect{case Some(b) => b}.size == putSomeCount &&
          inserts.map(_._2).collect{case None => 1}.size == putNoneCount
  }

  property("MultiPut Some/None count matches") = forAll { (inserts: Map[Int, Option[Int]]) =>
        var multiPutSomeCount = 0
        var multiPutNoneCount = 0
        val reporter = new StatReporter[Int, Int] {
          override def multiPutSome:Unit = multiPutSomeCount += 1
          override def multiPutNone:Unit = multiPutNoneCount += 1
        }
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        val wrappedStore = MergableStatStore[Int, Int](baseStore, reporter)


        wrappedStore.multiPut(inserts)

        inserts.map(_._2).collect{case Some(b) => b}.size == multiPutSomeCount &&
          inserts.map(_._2).collect{case None => 1}.size == multiPutNoneCount
  }


  property("merge Some/None count matches") = forAll { (base: Map[Int, Int], merge: Map[Int, Int]) =>
        var mergeWithSomeCount = 0
        var mergeWithNoneCount = 0
        val reporter = new StatReporter[Int, Int] {
          override def mergeWithSome:Unit = mergeWithSomeCount += 1
          override def mergeWithNone:Unit = mergeWithNoneCount += 1
        }
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        baseStore.multiMerge(base)
        val wrappedStore = MergableStatStore[Int, Int](baseStore, reporter)


        merge.map(kv => wrappedStore.merge((kv._1, kv._2)))

        val existsBeforeList = merge.keySet.toList.map(k => base.get(k))

        existsBeforeList.collect{case Some(_) => 1}.size == mergeWithSomeCount &&
          existsBeforeList.collect{case None => 1}.size == mergeWithNoneCount
  }

  property("multiMerge Some/None count matches") = forAll { (base: Map[Int, Int], merge: Map[Int, Int]) =>
        var mergeWithSomeCount = 0
        var mergeWithNoneCount = 0
        val reporter = new StatReporter[Int, Int] {
          override def multiMergeWithSome:Unit = mergeWithSomeCount += 1
          override def multiMergeWithNone:Unit = mergeWithNoneCount += 1
        }
        val baseStore = MergeableStore.fromStore(newStore[Int, Int])
        baseStore.multiMerge(base)
        val wrappedStore = MergableStatStore[Int, Int](baseStore, reporter)


        wrappedStore.multiMerge(merge)

        val existsBeforeList = merge.keySet.toList.map(k => base.get(k))

        existsBeforeList.collect{case Some(_) => 1}.size == mergeWithSomeCount &&
          existsBeforeList.collect{case None => 1}.size == mergeWithNoneCount
  }


}



//     /**
//     * get returns none when not in either store
//     */

//   def buildStoreRunQueries[K, V](mA: Map[K, V], others: Set[K], reporter: StatReporter[K, V]) = {
//     val baseStore = ReadableStore.fromMap(mA)
//     val wrappedStore = ReadableStatStore(baseStore, reporter)
//     val expanded: Set[K] = (mA.keySet ++ others)
//     // We use call to list, or it keeps the results of the map as a set and we loose data
//     expanded.toList.map{k: K => (mA.get(k), Await.result(wrappedStore.get(k)))}
//   }

//   def buildStoreRunMultiGetQueries[K, V](mA: Map[K, V], others: Set[K], reporter: StatReporter[K, V]) = {
//     val baseStore = ReadableStore.fromMap(mA)
//     val wrappedStore = ReadableStatStore(baseStore, reporter)
//     val expanded: Set[K] = (mA.keySet ++ others)
//     // We use call to list, or it keeps the results of the map as a set and we loose data
//     (expanded.map{k => (k, mA.get(k))}.toMap,
//       wrappedStore.multiGet(expanded).map{case (k, futureV) => (k, Await.result(futureV))})
//   }

//   property("Stats store matches raw get for all queries") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//         val reporter = new StatReporter[Int, String] {}
//         val queryResults = buildStoreRunQueries(mA, others, reporter)
//         queryResults.forall{case (a, b) => a == b}
//   }

//   property("Present/Absent count matches") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//         var presentCount = 0
//         var absentCount = 0
//         val reporter = new StatReporter[Int, String] {
//           override def getPresent:Unit = presentCount += 1
//           override def getAbsent:Unit = absentCount += 1

//         }
//         val queryResults = buildStoreRunQueries(mA, others, reporter)
//         val wrappedResults = queryResults.map(_._2)
//         val referenceResults = queryResults.map(_._2)
//         wrappedResults.collect{case Some(b) => b}.size == presentCount
//         wrappedResults.collect{case None => 1}.size == absentCount
//   }

//   property("traceGet can piggyback without breaking the results, hit counter is as expected") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//         var hitCounter = 0
//         val reporter = new StatReporter[Int, String] {
//           override def traceGet(request: Future[Option[String]]): Future[Option[String]] = request.onSuccess{ _ => hitCounter += 1}
//         }
//         val queryResults = buildStoreRunQueries(mA, others, reporter)
//         queryResults.forall{case (a, b) => a == b}
//         queryResults.size == hitCounter
//   }


//   property("Stats store matches raw get for multiget all queries") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//         val reporter = new StatReporter[Int, String] {}
//         val (mapRes, storeResults) = buildStoreRunMultiGetQueries(mA, others, reporter)
//         mapRes.size == storeResults.size &&
//           mapRes.keySet.forall(k => mapRes.get(k) == storeResults.get(k))
//   }

//   property("Present/Absent count matches in multiget") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//         var presentCount = 0
//         var absentCount = 0
//         val reporter = new StatReporter[Int, String] {
//           override def multiGetPresent:Unit = presentCount += 1
//           override def multiGetAbsent:Unit = absentCount += 1

//         }
//         val (_, storeResults) = buildStoreRunMultiGetQueries(mA, others, reporter)
//         storeResults.values.collect{case Some(b) => b}.size == presentCount
//         storeResults.values.collect{case None => 1}.size == absentCount
//   }

//   property("traceMultiGet can piggyback without breaking the results, hit counter is as expected") = forAll { (mA: Map[Int, String], others: Set[Int]) =>
//       var hitCounter = 0
//       val reporter = new StatReporter[Int, String] {
//         override def traceMultiGet[K1 <: Int](request: Map[K1, Future[Option[String]]]): Map[K1, Future[Option[String]]] = {
//           hitCounter += 1
//           request
//         }
//       }
//       val (mapRes, storeResults) = buildStoreRunMultiGetQueries(mA, others, reporter)
//       mapRes.size == storeResults.size &&
//         mapRes.keySet.forall(k => mapRes.get(k) == storeResults.get(k))
//       hitCounter == 1
//   }

// }
