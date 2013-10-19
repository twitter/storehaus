/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.storehaus.algebra

import com.twitter.algebird._
import com.twitter.util.{Promise, Try}

/**
 * TODO: REMOVE WHEN NEXT VERSION OF ALGEBIRD IS PUBLISHED
 * https://github.com/twitter/storehaus/issues/157
 * This Monoid allows code to depend on the result of computation asynchronously.
 * This is a slightly less general version of the TunnelMonoid. See the documentation
 * for TunnelMonoid for general motivation. NOTE: the Promise will be fulfilled with
 * the value just before the PromiseLink is calculated.
 */
class PromiseLinkMonoid[V](monoid: Monoid[V]) extends Monoid[PromiseLink[V]] { //TODo(jcoveney) rename PromiseLink
	def zero = PromiseLink(new Promise, monoid.zero)

  def plus(older: PromiseLink[V], newer: PromiseLink[V]): PromiseLink[V] = {
    val (PromiseLink(p1, v1), PromiseLink(p2, v2)) = (older, newer)
    p1.respond { tryOptV =>
      p2.update(tryOptV.map(optV => optV.map(monoid.plus(_, v1))))
    }
    PromiseLink(p1, monoid.plus(v1, v2))
  }

  override def isNonZero(v: PromiseLink[V]) = monoid.isNonZero(v.value)
}

/**
 * This class allows code to depends on the data that a value will be combined with,
 * fulfilling the Promise with the value just before the value is added in.
 */
case class PromiseLink[V](promise: Promise[Option[V]], value: V) {
  def completeWithStartingValue(startingV: Try[Option[V]])(implicit monoid: Monoid[V]): Try[V] = {
    promise.update(startingV)
    startingV.map { opt => opt.map(monoid.plus(_, value)).getOrElse(value) }
  }
}

object PromiseLink {
  implicit def monoid[V](implicit innerMonoid: Monoid[V]): PromiseLinkMonoid[V] =
    new PromiseLinkMonoid[V](innerMonoid)

	def toPromiseLink[V](value:V) = PromiseLink(new Promise[Option[V]], value)
}
