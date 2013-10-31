
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
