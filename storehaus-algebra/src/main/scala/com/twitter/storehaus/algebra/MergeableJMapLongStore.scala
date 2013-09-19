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

import java.util.concurrent.atomic.AtomicLong
import java.util.{ Map => JMap, HashMap => JHashMap }

import com.twitter.algebird.Monoid
import com.twitter.util.Future

/**
 * MergeableStore instance that is backed by a Java Map
 * and AtomicLong instances. This combination allows for
 * the associative addition necessary for the merge operation.
 * There is no optimization and all multi operations default to
 * the default Store implementation.
 *
 * This class is ideal for local testing of code that interacts with
 * a MergeableStore[String,Long] such as RedisLongStore without the need to
 * hit a remote database.
 *
 */
class MergeableJMapLongStore[K] extends MergeableStore[K, Long] {
  val monoid = implicitly[Monoid[Long]]

  private val dataContainer: JMap[K, AtomicLong] = new JHashMap[K, AtomicLong]()

  override def get(k: K): Future[Option[Long]] = {
    Future {
      Option(dataContainer.get(k)).map(_.get)
    }
  }

  override def put(kv: (K, Option[Long])): Future[Unit] = {
    Future {
      kv match {
        case (key, Some(value)) => {
          val atomicValue = new AtomicLong(value)
          dataContainer.put(key, atomicValue)
        }
        case (key, None) => {
          dataContainer.remove(key)
        }
      }
    }
  }

  override def merge(kv: (K, Long)): Future[Unit] = {
    Future {
      Option(dataContainer.get(kv._1)) match {
        case Some(value) => value.addAndGet(kv._2)
        case None => this.put(kv._1, Some(kv._2))
      }
    }
  }
}
