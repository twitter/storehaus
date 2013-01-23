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

import com.twitter.algebird.{ Semigroup, Monoid }
import com.twitter.storehaus.ReadableStore

object ReadableStoreMonoid {
  implicit def apply[K, V: Semigroup]: Monoid[ReadableStore[K, V]] =
    new ReadableStoreMonoid[K, V]
}

class ReadableStoreMonoid[K, V: Semigroup] extends Monoid[ReadableStore[K, V]] {
  import FutureAlgebra._

  override def zero: ReadableStore[K, V] = ReadableStore.empty[K, V]
  override def plus(l: ReadableStore[K, V], r: ReadableStore[K,V]): ReadableStore[K,V] =
    new ReadableStore[K,V] {
      override def get(k: K) = Semigroup.plus(l.get(k), r.get(k))
      override def multiGet(ks: Set[K]) = Semigroup.plus(l.multiGet(ks), r.multiGet(ks))
    }
}
