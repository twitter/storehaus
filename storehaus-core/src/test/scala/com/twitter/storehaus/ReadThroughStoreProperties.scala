/*
 * Copyright 2014 Twitter Inc.
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

object ReadThroughStoreProperties extends Properties("ReadThroughStoreProperties") {
  import ReadableStoreProperties.readableStoreLaws

  property("ReadThroughStore obeys the ReadableStore laws") =
    readableStoreLaws[String, Int] { m =>
      new ReadThroughStore(ReadableStore.fromMap(m), new ConcurrentHashMapStore[String, Int])
    }

  property("ReadThroughStore should ignore exceptions on the cache-store") =
    readableStoreLaws[String, Int] { m =>
      new ReadThroughStore(ReadableStore.fromMap(m),
        new ExceptionStore())
    }
}

