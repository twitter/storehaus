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

import com.twitter.util.Future
import com.twitter.storehaus.cache.{ Cache, MapCache, MutableCache }
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import scala.collection.mutable.{ Map => MutableMap }

object CachedReadableStoreProperties extends Properties("CachedReadableStoreProperties") {
  import ReadableStoreProperties.readableStoreLaws

  property("CachedReadableStore with empty beginning cache obeys the ReadableStore laws") =
    readableStoreLaws[String, Int] { m =>
      val cache = MapCache.empty[String, Future[Option[Int]]].toMutable()
      new CachedReadableStore(ReadableStore.fromMap(m), cache)
    }

  property("CachedReadableStore with full beginning cache obeys the ReadableStore laws") =
    readableStoreLaws[String, Int] { m =>
      val cache = MutableCache.fromMap((MutableMap.empty ++ m).map { case (k, v) => k -> Future.value(Option(v)) })
      new CachedReadableStore(ReadableStore.empty, cache)
    }
}
