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

import org.scalacheck.Properties
import org.scalacheck.Prop._

import com.twitter.util.{JavaTimer, Duration, Try}
import java.util.concurrent.TimeUnit.MILLISECONDS

object BatchedReadableStoreProperties extends Properties("BatchedReadableStoreProperties") {
  import ReadableStoreProperties.readableStoreLaws

  property("BatchedReadableStore obeys the ReadableStore laws") =
    readableStoreLaws[String, Int] { m =>
      new BatchedReadableStore(ReadableStore.fromMap(m), 3, 3)
    }
  property("MinBatchingReadableStore obeys the ReadableStore laws with min 1") =
    readableStoreLaws[String, Int] { m =>
      // This should work without any flushing, each get calls through
      new MinBatchingReadableStore(ReadableStore.fromMap(m), 1)
    }

  property("MinBatchingReadableStore obeys the ReadableStore laws with min 5") =
    readableStoreLaws[String, Int] { m =>
      val timer = new JavaTimer()
      // We need to flush periodically
      val s = new MinBatchingReadableStore(ReadableStore.fromMap(m), 5)
      // We need a lot of flushes because the tests do a lot of blocking
      timer.schedule(Duration(10, MILLISECONDS)) { s.flush }
      s
    }
}
