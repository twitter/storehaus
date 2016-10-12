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

package com.twitter.storehaus.testing.generator

import org.scalacheck.Gen._
import org.scalacheck.Gen

/** Generators for non-empty data */
object NonEmpty {
  /** Generator for non-empty alpha strings of random length */
  def alphaStr: Gen[String] =
    for (cs <- Gen.listOfN(1, Gen.alphaChar)) yield cs.mkString

  /** Generator for Options of non-empty alpha strings of random length */
  def alphaStrOpt: Gen[Option[String]] =
    alphaStr.flatMap(str => Gen.oneOf(Some(str), None))

  /** Generator for non-empty byte arrays of random length */
  def byteArray: Gen[Array[Byte]] =
    for (cs <- Gen.listOfN(1, Gen.alphaChar)) yield cs.mkString.getBytes("UTF-8")

  /** Generator for Options of non-empty by arrays of random length */
  def byteArrayOpt: Gen[Option[Array[Byte]]] =
    byteArray.flatMap(array => Gen.oneOf(Some(array), None))

  /** Storehaus pairings of non-empty data.
   *  In most cases this means 2 element tuples of(K, Option[V]) */
  object Pairing {
    /** Generator for pairings of non-empty alpha strings and non-empty alpha str options */
    def alphaStrPair: Gen[(String, Option[String])] = for {
      str <- NonEmpty.alphaStr
      opt <- NonEmpty.alphaStrOpt
    } yield (str, opt)

    /** Generator for pairings of non-empty byte arrays and non-empty byte arrays options */
    def byteArrayPair: Gen[(Array[Byte], Option[Array[Byte]])] = for {
      array <- NonEmpty.byteArray
      opt <- NonEmpty.byteArrayOpt
    } yield (array, opt)

    /** Generator for pairings of non-empty alpha strings to options of positive numerics */
    def alphaStrPosNumericPair[T : Numeric : Choose]: Gen[(String, Option[T])] = for {
      str <- NonEmpty.alphaStr
      opt <- Gen.posNum[T].flatMap(l => Gen.oneOf(Some(l), None))
    } yield (str, opt)

    /** Generator for pairings of numerics */
    def numericPair[T : Numeric : Choose]: Gen[(T, Option[T])] = for {
      num <- Gen.posNum[T]
      opt <- Gen.posNum[T].flatMap(l => Gen.oneOf(Some(l), None))
    } yield (num, opt)

    /** Generator for non-empty lists of (String, Option[String])'s */
    def alphaStrs(n: Int = 10): Gen[List[(String, Option[String])]] =
      Gen.listOfN(n, alphaStrPair)

    /** Generator for non-empty lists of (Array[Byte], Option[Array[Byte]])'s */
    def byteArrays(n: Int = 10): Gen[List[(Array[Byte], Option[Array[Byte]])]] =
      Gen.listOfN(n, byteArrayPair)

    /** Generator for non-empty lists of (String, Option[T])'s */
    def alphaStrNumerics[T : Numeric : Choose](n: Int = 10): Gen[List[(String, Option[T])]] =
      Gen.listOfN(n, alphaStrPosNumericPair[T])

    /** Genrator for non-empty lists of numerics (T, Option[T])'s */
    def numerics[T : Numeric : Choose](n: Int = 10): Gen[List[(T, Option[T])]] =
      Gen.listOfN(n, numericPair[T])
  }
}
