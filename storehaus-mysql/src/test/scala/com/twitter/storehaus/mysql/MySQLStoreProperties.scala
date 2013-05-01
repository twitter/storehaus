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

package com.twitter.storehaus.mysql

import java.util.logging.Level

import com.twitter.finagle.exp.mysql.Client
import com.twitter.util.Await

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object MySqlStoreProperties extends Properties("MySqlStore") {

  // used to generate arbitrary pairs of types we want to test
  def validPairs[T: Arbitrary] = Arbitrary.arbitrary[List[(T, Option[T])]] suchThat(!_.isEmpty)

  def put(s: MySqlStore, pairs: List[(MySqlValue, Option[MySqlValue])]) {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def putAndGetStoreTest(store: MySqlStore, pairs: Gen[List[(Any, Option[Any])]] = validPairs[String]) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = examples.map { case (k, v) =>
          (MySqlStringInjection.invert(k.toString).get, v match {
            case Some(d) => Some(MySqlStringInjection.invert(d.toString).get)
            case None => None
          })
        }
      put(store, stringified)
      stringified.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def putAndMultiGetStoreTest(store: MySqlStore, pairs: Gen[List[(Any, Option[Any])]] = validPairs[String]) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = examples.map { case (k, v) =>
          (MySqlStringInjection.invert(k.toString).get, v match {
            case Some(d) => Some(MySqlStringInjection.invert(d.toString).get)
            case None => None
          })
        }
      put(store, stringified)
      val data = stringified.toMap
      val result = store.multiGet(data.keySet)
      data.forall { case (k, optV) =>
        // result.get(k) returns Option[Future[Option[MySqlValue]]]
        val foundOptV = result.get(k) match {
          case Some(v) => Await.result(v)
          case None => None
        }
        compareValues(k, optV, foundOptV)
      }
    }

  def compareValues(k: MySqlValue, expectedOptV: Option[MySqlValue], foundOptV: Option[MySqlValue]) = {
    val isMatch = expectedOptV match {
      case Some(value) => !foundOptV.isEmpty && foundOptV.get == value 
      case None => foundOptV.isEmpty
    }
    if (!isMatch) printErr(k, expectedOptV, foundOptV)
    isMatch
  }

  def printErr(k: MySqlValue, expectedOptV: Option[MySqlValue], foundOptV: Option[MySqlValue]) {
    val expected = if (expectedOptV.isEmpty) { expectedOptV } else { "Some("+MySqlStringInjection(expectedOptV.get)+")" }
    val found = if (foundOptV.isEmpty) { foundOptV } else { "Some("+MySqlStringInjection(foundOptV.get)+")" }
    println("FAILURE: Key \""+MySqlStringInjection(k)+"\" - expected value "+expected+", but found "+found)
  }

  property("MySqlStore text->text") =
    withStore(putAndGetStoreTest(_), "text", "text")

  property("MySqlStore blob->blob") =
    withStore(putAndGetStoreTest(_), "blob", "blob")

  property("MySqlStore text->blob") =
    withStore(putAndGetStoreTest(_), "text", "blob")

  property("MySqlStore text->text multiget") =
    withStore(putAndMultiGetStoreTest(_), "text", "text", true)

  property("MySqlStore blob->blob multiget") =
    withStore(putAndMultiGetStoreTest(_), "blob", "blob", true)

  property("MySqlStore text->blob multiget") =
    withStore(putAndMultiGetStoreTest(_), "text", "blob", true)

  property("MySqlStore int->int") =
    withStore(putAndGetStoreTest(_, validPairs[Int]), "int", "int")

  property("MySqlStore int->int multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Int]), "int", "int", true)

  property("MySqlStore bigint->bigint") =
    withStore(putAndGetStoreTest(_, validPairs[Long]), "bigint", "bigint")

  property("MySqlStore bigint->bigint multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Long]), "bigint", "bigint", true)

  property("MySqlStore smallint->smallint") =
    withStore(putAndGetStoreTest(_, validPairs[Short]), "smallint", "smallint")

  property("MySqlStore smallint->smallint multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Short]), "smallint", "smallint", true)
  
  private def withStore[T](f: MySqlStore => T, kColType: String, vColType: String, multiGet: Boolean = false): T = {
    val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test", Level.WARNING)
    // these should match mysql setup used in .travis.yml

    val tableName = "storehaus-mysql-"+kColType+"-"+vColType + ( if (multiGet) { "-multiget" } else { "" } )
    val schema = "CREATE TEMPORARY TABLE IF NOT EXISTS `"+tableName+"` (`key` "+kColType+" DEFAULT NULL, `value` "+vColType+" DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))

    val store = MySqlStore(client, tableName, "key", "value")
    val result = f(store)
    store.close
    result
  }
}
