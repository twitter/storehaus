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

package com.twitter.storehaus.testing

import org.scalacheck.Gen

object Generators {

  /** Generator for non-empty alpha strings of random length */
  def nonEmptyAlphaStr: Gen[String] =
   for(cs <- Gen.listOf1(Gen.alphaChar)) yield cs.mkString

  /** Generator for Options of non-empty alpha strings of random length */
  def nonEmptyAlphaStrOpt: Gen[Option[String]] =
    nonEmptyAlphaStr.flatMap(str => Gen.oneOf(Some(str), None))

  /** Generator for Options of postive long values */
  def posLongOpt: Gen[Option[Long]] =
    Gen.posNum[Long].flatMap(l => Gen.oneOf(Some(l), None))
}
