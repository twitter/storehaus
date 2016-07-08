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

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import scala.util.{Try, Success, Failure}

/**
  * Injection that maps values paired with stale values of T => None
  * on inversion.
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  */

case class ExpiredException[K, V](pair: (K, V)) extends RuntimeException(pair.toString)

class TTLInjection[K, T: Ordering: Semigroup, V](delta: T)(clock: () => T)
    extends Injection[(K, V), (K, (T, V))] {
  def apply(pair: (K, V)): (K, (T, V)) = {
    val (k, v) = pair
    (k, (Semigroup.plus(clock(), delta), v))
  }

  override def invert(pair: (K, (T, V))): Try[(K, V)] = {
    val (k, (expiration, v)) = pair
    if (Ordering[T].gteq(expiration, clock())) Success(k -> v)
    else Failure(ExpiredException(k -> v))
  }
}
