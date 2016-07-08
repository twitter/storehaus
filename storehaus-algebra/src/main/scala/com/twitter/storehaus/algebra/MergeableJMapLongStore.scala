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

import java.util.concurrent.{ ConcurrentHashMap => JConcurrentHashMap }

import com.twitter.algebird.Semigroup
import com.twitter.util.Future

/**
 * MergeableStore instance that is backed by a ConcurrentHashMap
 * This class is thread safe with a locking merge operation
 * and thread safe put/get operations provided by the underlying
 * ConcurrentHashMap
 * There is no multi operation optimization so all multi operations use
 * the default Store implementation.
 *
 * This class is ideal for local testing of code that interacts with
 * a MergeableStore[String,Long] such as RedisLongStore without the need to
 * hit a remote database. Can also be used for data processing where
 * you don't care about persistence beyond the running process.
 *
 */
class MergeableJMapLongStore[K] extends MergeableJMapStore[K, Long]()(implicitly[Semigroup[Long]])

class MergeableJMapStore[K, V](implicit override val semigroup: Semigroup[V])
  extends MergeableStore[K, V] {

  protected val dataContainer = new JConcurrentHashMap[K, V]

  override def get(k: K): Future[Option[V]] = {
    Future.value {
      Option(dataContainer.get(k))
    }
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    Future.value {
      kv match {
        case (key, Some(value)) =>
          dataContainer.put(key, value)
        case (key, None) =>
          dataContainer.remove(key)
      }
    }
  }

  override def merge(kv: (K, V)): Future[Option[V]] = {
    Future.value {
      this.synchronized {
        Option(dataContainer.get(kv._1)) match {
          case start@Some(value) =>
            dataContainer.put(kv._1, Semigroup.plus(value, kv._2))
            start
          case None =>
            this.put((kv._1, Some(kv._2)))
            None
        }
      }
    }
  }
}
