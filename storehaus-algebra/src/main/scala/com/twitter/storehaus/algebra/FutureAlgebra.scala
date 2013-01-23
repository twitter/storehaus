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
import com.twitter.util.Future

/**
 * Monoid and Semigroup on util.Future.
 *
 * @author Sam Ritchie
 */

object FutureAlgebra {
  implicit def semigroup[T: Semigroup] = new FutureSemigroup[T]
  implicit def monoid[T: Monoid] = new FutureMonoid[T]
}

class FutureSemigroup[T: Semigroup] extends Semigroup[Future[T]] {
  override def plus(l: Future[T], r: Future[T]): Future[T] =
    l.join(r).map { case (l, r) => Semigroup.plus(l, r) }
}

class FutureMonoid[T: Monoid] extends FutureSemigroup[T] with Monoid[Future[T]] {
  override def zero = Future.value(Monoid.zero)
}
