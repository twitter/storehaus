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
object Injections {
  private implicit val formats = native.Serialization.formats(NoTypeHints)

  /**
   * Case Class to Json Injection
   * @tparam A Case Class
   * @return Json String
   */
  implicit def caseClass2Json[A <: Product : Manifest]: Injection[A, String] = new AbstractInjection[A, String] {
    override def apply(a: A): String = write(a)

    override def invert(b: String): Try[A] = attempt(b)(read[A])
  }
}
