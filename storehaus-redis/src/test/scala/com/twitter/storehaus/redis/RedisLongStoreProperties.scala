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

package com.twitter.storehaus.redis

import com.twitter.bijection.Injection
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.storehaus.{ FutureOps, Store }
import com.twitter.storehaus.algebra.ConvertedStore
import com.twitter.storehaus.redis.RedisStoreProperties.{ putStoreTest, multiPutStoreTest }
import com.twitter.storehaus.testing.{ CloseableCleanup, Generators }
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.listOf1
import org.scalacheck.Prop._
import org.scalacheck.Properties

object RedisLongStoreProperties extends Properties("RedisLongStore")
  with CloseableCleanup[Store[String, Long]]
  with DefaultRedisClient {

  def paired: Gen[(String, Option[Long])] = for {
    str <- Generators.nonEmptyAlphaStr
    opt <- Generators.posLongOpt
  } yield (str, opt)

  def validPairs: Gen[List[(String, Option[Long])]] = Gen.listOfN(10,paired)

  def storeTest(store: Store[String, Long]) =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val closeable =
    RedisLongStore(client).convert(StringToChannelBuffer(_: String))

  property("RedisLongStore test") =
    storeTest(closeable)
}
