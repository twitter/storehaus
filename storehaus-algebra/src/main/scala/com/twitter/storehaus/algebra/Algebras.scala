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

import com.twitter.algebird.{ Semigroup, Monoid, Group, Ring, Field }
import com.twitter.algebird.util.UtilAlgebras
import com.twitter.storehaus.{ReadableStore, AbstractReadableStore}
import com.twitter.util.Future

import UtilAlgebras._

object Algebras {
  implicit def semigroup[K, V: Semigroup]: Semigroup[ReadableStore[K, V]] = new ReadableStoreSemigroup[K, V]
  implicit def monoid[K, V: Monoid]: Monoid[ReadableStore[K, V]] = new ReadableStoreMonoid[K, V]
}

class ReadableStoreSemigroup[K, V: Semigroup] extends Semigroup[ReadableStore[K, V]] {
  override def plus(l: ReadableStore[K, V], r: ReadableStore[K,V]) =
    new AbstractReadableStore[K,V] {
      override def get(k: K) = Semigroup.plus(l.get(k), r.get(k))
      override def multiGet[K1<:K](ks: Set[K1]) = Semigroup.plus(l.multiGet(ks), r.multiGet(ks))
    }
}

class ReadableStoreMonoid[K, V: Monoid] extends ReadableStoreSemigroup[K, V] with Monoid[ReadableStore[K, V]] {
  override def zero = ReadableStore.const(Monoid.zero[V])
}
