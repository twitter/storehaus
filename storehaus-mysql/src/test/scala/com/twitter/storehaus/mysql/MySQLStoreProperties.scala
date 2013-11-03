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
import com.twitter.storehaus.testing.SelfAggregatingCloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Await, Future}

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object MySqlStoreProperties extends Properties("MySqlStore")
  with SelfAggregatingCloseableCleanup[MySqlStore] {

  def put(s: MySqlStore, pairs: List[(MySqlValue, Option[MySqlValue])]) {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def multiPut(s: MySqlStore, pairs: List[(MySqlValue, Option[MySqlValue])]) {
    Await.result(Future.collect(s.multiPut(pairs.toMap, true /* batched */).values.toSeq))
  }

  /** invert any type to MySql String values. Because most mysql configuraions are case insensitive by default,
   *  we lowercase key's here for normalization */
  def stringify(examples: List[(Any, Option[Any])]) =
    examples.map { case (k, v) =>
      (MySqlStringInjection.invert(k.toString.toLowerCase).get, v.flatMap { d => MySqlStringInjection.invert(d.toString).toOption })
    }

  def putAndGetStoreTest(store: MySqlStore, pairs: Gen[List[(Any, Option[Any])]] = NonEmpty.Pairing.alphaStrs()) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = stringify(examples)
      put(store, stringified)
      stringified.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def multiPutAndMultiGetStoreTest(store: MySqlStore, pairs: Gen[List[(Any, Option[Any])]] = NonEmpty.Pairing.alphaStrs()) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = stringify(examples)
      multiPut(store, stringified)
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
    withStore(multiPutAndMultiGetStoreTest(_), "text", "text", true)

  property("MySqlStore blob->blob multiget") =
    withStore(multiPutAndMultiGetStoreTest(_), "blob", "blob", true)

  property("MySqlStore text->blob multiget") =
    withStore(multiPutAndMultiGetStoreTest(_), "text", "blob", true)

  property("MySqlStore int->int") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Int]()), "int", "int")

  property("MySqlStore int->int multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Int]()), "int", "int", true)

  property("MySqlStore bigint->bigint") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Long]()), "bigint", "bigint")

  property("MySqlStore bigint->bigint multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Long]()), "bigint", "bigint", true)

  property("MySqlStore smallint->smallint") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Short]()), "smallint", "smallint")

  property("MySqlStore smallint->smallint multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Short]()), "smallint", "smallint", true)

  private def withStore[T](f: MySqlStore => T, kColType: String, vColType: String, multiGet: Boolean = false): T = {
    val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test", Level.WARNING)
    // these should match mysql setup used in .travis.yml

    val tableName = "storehaus-mysql-"+kColType+"-"+vColType + ( if (multiGet) { "-multiget" } else { "" } )
    val schema = "CREATE TEMPORARY TABLE IF NOT EXISTS `"+tableName+"` (`key` "+kColType+" DEFAULT NULL, `value` "+vColType+" DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))

    f(newStore(client, tableName))
  }

  def newStore(client: Client, tableName: String) =
    aggregateCloseable(MySqlStore(client, tableName, "key", "value"))
}
