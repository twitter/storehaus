/*
* Copyright 2013 Twitter Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License. You may obtain
* a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.twitter.storehaus.memcache

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.bijection.netty.ChannelBufferBijection
import com.twitter.finagle.memcached.Client
import com.twitter.storehaus.testing.SelfAggregatingCloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.Await

import org.jboss.netty.buffer.ChannelBuffer
import org.scalacheck.Gen
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

/** Unit test using Long values */
object MergeableMemcacheStoreProperties extends Properties("MergeableMemcacheStore")
  with SelfAggregatingCloseableCleanup[MergeableMemcacheStore[String, Long]] {

  def put(s: MergeableMemcacheStore[String, Long], pairs: List[(String, Option[Long])]) = {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def merge(s: MergeableMemcacheStore[String, Long], kvs: Map[String, Long]) = s.multiMerge(kvs)

  def putAndGetStoreTest(store: MergeableMemcacheStore[String, Long],
      pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)) =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def mergeStoreTest(store: MergeableMemcacheStore[String, Long],
      pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)) =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      put(store, examples)
      // increment values
      merge(store, examples.map { case (k, v) => (k, 1l) }.toMap).forall { case (k, futureOptV) =>
        val foundOptV = Await.result(futureOptV)
        // verify old values are returned
        compareValues(k, examples.toMap.get(k).get, foundOptV)
      }
      // now verify incremented values are returned
      examples.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV.map { _ + 1l }.orElse(Some(1l)), foundOptV)
      }
    }

  def compareValues(k: String, expectedOptV: Option[Long], foundOptV: Option[Long]) = {
    val isMatch = expectedOptV match {
      case Some(value) => !foundOptV.isEmpty && foundOptV.get == value
      case None => foundOptV.isEmpty
    }
    if (!isMatch)
      println("FAILURE: Key \"" + k + "\" - expected value " + expectedOptV + ", but found " + foundOptV)
    isMatch
  }

  property("MergeableMemcacheStore put, get and merge") = {
    implicit val cb2ary = ChannelBufferBijection
    val client = Client("localhost:11211")
    val injection = Injection.connect[Long, String, Array[Byte], ChannelBuffer]
    val semigroup = implicitly[Semigroup[Long]]
    val store = MergeableMemcacheStore[String, Long](client)(identity)(injection, semigroup)

    putAndGetStoreTest(store) && mergeStoreTest(store)
  }
}

