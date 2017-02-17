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

import com.twitter.finagle.redis.util.StringToBuf
import com.twitter.storehaus.Store
import com.twitter.storehaus.redis.RedisStoreProperties.{multiPutStoreTest, putStoreTest}
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import org.scalacheck.{Gen, Prop, Properties}

object RedisLongStoreProperties extends Properties("RedisLongStore")
  with CloseableCleanup[Store[String, Long]]
  with DefaultRedisClient {

  def validPairs: Gen[List[(String, Option[Long])]] =
    NonEmpty.Pairing.alphaStrNumerics[Long](10)

  def storeTest(store: Store[String, Long]): Prop =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val closeable : Store[String, Long] =
    RedisLongStore(client).convert(StringToBuf(_: String))

  property("RedisLongStore test") = storeTest(closeable)
}
