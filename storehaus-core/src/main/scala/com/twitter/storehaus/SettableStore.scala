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

import scala.math.Equiv
import com.twitter.util.Future

trait SettableStore[K, V] extends ReadableStore[K, V] {
  def equiv: Equiv[V]
  // Returns the value before this set
  def set(kv: (K,V)): Future[V]
  def multiSet(kvs: Map[K,V]): Future[Map[K,V]]
  // if the value is the first, set, otherwise do nothing. return initial value
  // TODO: def compareAndSet(kv: (K,(V,V))): Future[V]
  // TODO: def multiCompareAndSet(kvs: Map[K,(V,V)]): Future[Map[K,V]]
}
