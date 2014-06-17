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
package com.twitter.storehaus.cache

/** Defines the base of a proxy for a given type.
 *  A instance of Proxied for type T is intended to use the `self`
 *  member to forward all methods of an new instance of type T to.
 *  This allows for extensions of type T which can inherit a proxied
 *  instance's behavior without needing to override every method of type T.
 *
 *  {{{
 *
 *  class Dazzle {
 *    def a: String = "default a"
 *    def b: String = "default b"
 *    // ...
 *  }
 *
 *  // define a reusable concrete proxy statisfying Dazzle forwarding
 *  // all calls to Proxied method self
 *  class DazzleProxy(val self: Dazzle) extends Dazzle with Proxied[Dazzle] {
 *    def a: String = self.a
 *    def b: String = self.b
 *  }
 *
 *  val bedazzlable = new Dazzle {
 *    // return a new Dazzle with some sizzle
 *    def be(sizzle: String): Dazzle = new DazzleProxy(this) {
 *      override def b = "%s %s!!!" format(self.b, sizzle)
 *    }
 *  }
 *
 *  val dazzled = bedazzlable.be("dazzled")
 *  dazzled.b // default b dazzled!!!
 *  dazzled.a // default a
 *
 *  }}}
 *
 *  @author Doug Tangren
 */
trait CacheProxied[T] {
  protected def self: T
}