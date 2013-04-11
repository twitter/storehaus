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

import com.twitter.storehaus.cache.MutableCache
import com.twitter.util.Future

/**
  * Generates a [[com.twitter.storehaus.Store]] backed by a
  * [[com.twitter.storehaus.cache.MutableCache]].
  */

class CacheStore[K, V](cache: MutableCache[K, V]) extends Store[K, V] {
  override def get(k: K): Future[Option[V]] = Future.value(cache.get(k))
  override def put(kv: (K, Option[V])): Future[Unit] = {
    kv match {
      case (k, Some(v)) => cache += (k -> v)
      case (k, None) => cache -= k
    }
    Future.Unit
  }
}
