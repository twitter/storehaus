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

import com.twitter.conversions.time._
import com.twitter.util.{JavaTimer, Timer}
import java.util.Random
import org.scalacheck.Properties

object RetryingReadableStoreProperties extends Properties("RetryingStore") {
  import ReadableStoreProperties.readableStoreLaws

  val random = new Random(System.currentTimeMillis)
  implicit val timer: Timer = new JavaTimer(true)

  val fromMapWithRandomLatency: Int => Map[Int, String] => Int => Option[String] =
    maxLatencyMillis => m => k => {
      val ranLatencyMillis = random.nextInt(maxLatencyMillis)
      Thread.sleep(ranLatencyMillis)
      m.get(k)
    }

  property("RetryingReadableStore obeys the ReadableStore laws, assuming the underlying ReadableStore always returns results before timeout") =
    readableStoreLaws[Int, String] { m =>
      ReadableStore.withRetry(
        store = ReadableStore.fromFn(fromMapWithRandomLatency(2)(m)),
        backoffs = for (i <- 0 until 3) yield 1.milliseconds
      )(_ => true)
    }
}
