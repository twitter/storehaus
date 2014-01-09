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
package com.twitter.storehaus.mongodb

import scala.util.Try

import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.bijection.Conversion.asMethod

import com.mongodb.casbah.Imports._

import org.scalacheck._

/**
 * @author Bin Lan
 */

case class Test(a: String, b: Int)

object MongoObjectStoreProperties extends Properties("MongoObjectStore") {
  implicit val testToMongoDBObject = new MongoInjection[Test] {
    override def apply(in: Test): MongoDBObject = MongoDBObject("a" -> in.a, "b" -> in.b)
    override def invert(in: MongoDBObject): Try[Test] = Try(Test(in.as[String]("a"), in.as[Int]("b")))
  }

  property("MongoObjectStore[String, Test]") =
    MongoStoreProperties.putAndGetStoreTest[String, Test](MongoObjectStore[String, Test](
        MongoClient("127.0.0.1", 27017),
        "storehaus",
        "data.objectMap"
      ), alphaObjects())

  def randObject(): Gen[Test] = for {
    str <- for (cs <- Gen.listOf1(Gen.alphaChar)) yield cs.mkString
    int <- Gen.posNum[Int]
  } yield Test(str, int)

  def randObjectOpt(): Gen[Option[Test]] = {
    randObject().flatMap(obj => {
      Gen.oneOf(Some(obj), None)
    })
  }

  def mongoTestObjectPair(): Gen[(String, Option[Test])] = for {
    str <- NonEmpty.alphaStr
    obj <- randObjectOpt()
  } yield (str, obj)

  def alphaObjects(n: Int = 10) =
    Gen.listOfN(n, mongoTestObjectPair)
}
