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

import com.twitter.bijection.{Bijection, AbstractBijection}
import com.twitter.util.{Future, Try, Return}

/** Bijections on objects in Twitter util
 * should be moved to a sub-module of bijection, (but they are very small)
 */
object UtilBijections {
  // Clearly we need a Monadic bijection that uses any object with a map,
  // but we need the Monad typeclass... why doesn't scala provide it

  /** Bijection on Future
   * if the bijection throws, the result will be a Throw.
   */
  implicit def futureBijection[A,B](implicit bij: Bijection[A,B]): Bijection[Future[A], Future[B]] =
    new AbstractBijection[Future[A], Future[B]] {
      override def apply(fa: Future[A]) = fa.flatMap { a => Future(bij(a)) }
      override def invert(fb: Future[B]) = fb.flatMap { b => Future(bij.invert(b)) }
    }
  /** Bijection on Try.
   * If the the bijection throws, the result will be a throw
   */
  implicit def tryBijection[A,B](implicit bij: Bijection[A,B]): Bijection[Try[A],Try[B]] =
    new AbstractBijection[Try[A], Try[B]] {
      override def apply(fa: Try[A]) = fa.flatMap { a => Try(bij(a)) }
      override def invert(fb: Try[B]) = fb.flatMap { b => Try(bij.invert(b)) }
    }
}
