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
import com.twitter.storehaus.redis.RedisStoreProperties.{ putStoreTest, multiPutStoreTest, validPairs }
import org.scalacheck.Properties
import org.scalacheck.Prop._

object RedisStringStoreProperties extends Properties("RedisStringStore")
  with CloseableCleanup[Store[String, String]] 
  with DefaultRedisClient {

  def storeTest(store: Store[String, String]) =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val closeable =
    RedisStringStore(client).convert(StringToChannelBuffer(_: String))

  property("RedisStringStore test") =
    storeTest(closeable)
}
