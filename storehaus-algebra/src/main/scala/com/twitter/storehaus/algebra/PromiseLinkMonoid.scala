/*
 * Copyright 2012 Twitter Inc.
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

import com.twitter.algebird._
import com.twitter.util.{Future, Promise, Try, Throw, Return}

class PromiseLinkSemigroup[V](sg: Semigroup[V]) extends Semigroup[PromiseLink[V]] {
  def plus(older: PromiseLink[V], newer: PromiseLink[V]): PromiseLink[V] = {
    val (PromiseLink(p1, v1), PromiseLink(p2, v2)) = (older, newer)
    p1.respond { tryOptV =>
      p2.update(tryOptV.map(optV => optV.map(sg.plus(_, v1))))
    }
    PromiseLink(p1, sg.plus(v1, v2))
  }
}
/**
 * TODO: REMOVE WHEN NEXT VERSION OF ALGEBIRD IS PUBLISHED
 * https://github.com/twitter/storehaus/issues/157
 * This Monoid allows code to depend on the result of computation asynchronously.
 * This is a slightly less general version of the TunnelMonoid. See the documentation
 * for TunnelMonoid for general motivation. NOTE: the Promise will be fulfilled with
 * the value just before the PromiseLink is calculated.
 */
class PromiseLinkMonoid[V](monoid: Monoid[V])
    extends PromiseLinkSemigroup[V](monoid) with Monoid[PromiseLink[V]] {
  def zero: PromiseLink[V] = PromiseLink(new Promise[Option[V]], monoid.zero)
  override def isNonZero(v: PromiseLink[V]): Boolean = monoid.isNonZero(v.value)
}

/**
 * This class allows code to depends on the data that a value will be combined with,
 * fulfilling the Promise with the value just before the value is added in.
 */
case class PromiseLink[V](promise: Promise[Option[V]], value: V) {
  def result(implicit sg: Semigroup[V]): Future[V] =
    promise.map { opt => opt.map(sg.plus(_, value)).getOrElse(value) }

  def completeIfEmpty(startingV: Try[Option[V]]): Boolean =
    promise.updateIfEmpty(startingV)

  /** Returns none if this was already set, else this plus the init
   */
  def apply(start: Option[V])(implicit semigroup: Semigroup[V]): Option[V] = {
    if (completeIfEmpty(Return(start))) {
      Some(start.map(semigroup.plus(_, value)).getOrElse(value))
    }
    else None
  }

  def fail(t: Throwable): Boolean = promise.updateIfEmpty(Throw(t))
}

object PromiseLink {
  implicit def semigroup[V](implicit innerSg: Semigroup[V]): PromiseLinkSemigroup[V] =
    new PromiseLinkSemigroup[V](innerSg)

  implicit def monoid[V](implicit innerMonoid: Monoid[V]): PromiseLinkMonoid[V] =
    new PromiseLinkMonoid[V](innerMonoid)

  def apply[V](value: V): PromiseLink[V] = PromiseLink[V](new Promise[Option[V]], value)
}
