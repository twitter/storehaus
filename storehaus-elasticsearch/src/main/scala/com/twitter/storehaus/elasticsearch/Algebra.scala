/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.elasticsearch

import com.twitter.bijection._
import scala.util.Try
import org.json4s._
import native.JsonMethods._
import com.twitter.bijection.Inversion.attempt
import native.Serialization.{write, read}
import com.twitter.algebird.Monoid
import org.json4s.JValue
import com.twitter.algebird.bijection.BijectedMonoid


/**
 * Temporary home of Json algebra. Will be removed once Json4s bijections get merged in twitter-bijections
 * @author Mansur Ashraf
 * @since 1/8/14
 */
object Algebra {
  implicit val formats = native.Serialization.formats(NoTypeHints)

  /**
   * JValue to Json Injection
   */
  implicit val jvalue2Json = new AbstractInjection[JValue, String] {
    override def apply(a: JValue): String = compact(render(a))

    override def invert(b: String): Try[JValue] = attempt(b)(parse(_))
  }

  /**
   * Case Class to Json Injection
   * @tparam A Case Class
   * @return Json String
   */
  implicit def cc2Json[A <: AnyRef : Manifest] = new AbstractInjection[A, String] {
    override def apply(a: A): String = write(a)

    override def invert(b: String): Try[A] = attempt(b)(read[A])
  }

  /**
   * Case Class to JValue Injection
   * @tparam A Case Class
   * @return JValue
   */
  implicit def cc2JValue[A <: AnyRef : Manifest] = new AbstractInjection[A, JValue] {
    override def apply(a: A): JValue = Extraction.decompose(a)

    override def invert(b: JValue): Try[A] = attempt(b)(_.extract[A])
  }

  implicit val jvalueMonoid = new Monoid[JValue] {
    def plus(l: JValue, r: JValue): JValue = l merge  r

    def zero: JValue = JNothing
  }

  private implicit val unsafeInj=Injection.unsafeToBijection(jvalue2Json)  //jank

  implicit val jsonMonoid = new BijectedMonoid[JValue, String]

}
