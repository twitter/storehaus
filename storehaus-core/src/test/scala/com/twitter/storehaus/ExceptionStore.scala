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

import com.twitter.util.Future

import scala.util.Random

class ExceptionStore[K, V](possibility: Float = 0.5f) extends ConcurrentHashMapStore[K, V] {
  private[this] def wrap[A](f: => Future[A]): Future[A] = {
    if (Random.nextFloat() < possibility) Future.exception(new RuntimeException())
    else f
  }

  override def get(k: K): Future[Option[V]] = wrap(super.get(k))

  override def put(kv: (K, Option[V])): Future[Unit] = wrap(super.put(kv))
}
