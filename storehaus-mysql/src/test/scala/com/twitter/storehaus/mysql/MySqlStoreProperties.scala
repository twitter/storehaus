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

import com.twitter.storehaus.mysql.compat.Mysql
import com.twitter.storehaus.mysql.compat.Client
import com.twitter.storehaus.testing.SelfAggregatingCloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Await, Future}

import org.scalacheck.{Prop, Gen, Properties}
import org.scalacheck.Prop.forAll

object MySqlStoreProperties extends Properties("MySqlStore")
  with SelfAggregatingCloseableCleanup[MySqlStore] {

  def put(s: MySqlStore, pairs: List[(MySqlValue, Option[MySqlValue])]) {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def multiPut(s: MySqlStore, pairs: List[(MySqlValue, Option[MySqlValue])]) {
    Await.result(Future.collect(s.multiPut(pairs.toMap).values.toSeq))
  }

  /** invert any type to MySql String values. Because most mysql configuraions are case
   * insensitive by default, we lowercase key's here for normalization */
  def stringify(examples: List[(Any, Option[Any])]): List[(MySqlValue, Option[MySqlValue])] =
    examples.map { case (k, v) =>
      (String2MySqlValueInjection(k.toString.toLowerCase),
        v.flatMap { d => Option(String2MySqlValueInjection(d.toString)) })
    }

  def putAndGetStoreTest(
    store: MySqlStore,
    pairs: Gen[List[(Any, Option[Any])]] = NonEmpty.Pairing.alphaStrs()
  ): Prop =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = stringify(examples)
      put(store, stringified)
      stringified.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def multiPutAndMultiGetStoreTest(
    store: MySqlStore,
    pairs: Gen[List[(Any, Option[Any])]] = NonEmpty.Pairing.alphaStrs()
  ): Prop =
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

  def compareValues(
      k: MySqlValue, expectedOptV: Option[MySqlValue], foundOptV: Option[MySqlValue]): Boolean = {
    val isMatch = expectedOptV match {
      case Some(value) => foundOptV.isDefined && foundOptV.get == value
      case None => foundOptV.isEmpty
    }
    if (!isMatch) printErr(k, expectedOptV, foundOptV)
    isMatch
  }

  def printErr(k: MySqlValue, expectedOptV: Option[MySqlValue], foundOptV: Option[MySqlValue]) {
    val expected =
      if (expectedOptV.isEmpty) expectedOptV
      else String2MySqlValueInjection.invert(expectedOptV.get).toOption
    val found =
      if (foundOptV.isEmpty) foundOptV
      else String2MySqlValueInjection.invert(foundOptV.get).toOption
    println(s"""FAILURE: Key "${String2MySqlValueInjection.invert(k)}" -""" +
      s"expected value $expected, but found $found")
  }

  property("MySqlStore text->text") =
    withStore(putAndGetStoreTest(_), "text", "text")

  property("MySqlStore blob->blob") =
    withStore(putAndGetStoreTest(_), "blob", "blob")

  property("MySqlStore text->blob") =
    withStore(putAndGetStoreTest(_), "text", "blob")

  property("MySqlStore text->text multiget") =
    withStore(multiPutAndMultiGetStoreTest(_), "text", "text", multiGet = true)

  property("MySqlStore blob->blob multiget") =
    withStore(multiPutAndMultiGetStoreTest(_), "blob", "blob", multiGet = true)

  property("MySqlStore text->blob multiget") =
    withStore(multiPutAndMultiGetStoreTest(_), "text", "blob", multiGet = true)

  property("MySqlStore int->int") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Int]()), "int", "int")

  property("MySqlStore int->int multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Int]()),
      "int", "int", multiGet = true)

  property("MySqlStore bigint->bigint") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Long]()), "bigint", "bigint")

  property("MySqlStore bigint->bigint multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Long]()),
      "bigint", "bigint", multiGet = true)

  property("MySqlStore smallint->smallint") =
    withStore(putAndGetStoreTest(_, NonEmpty.Pairing.numerics[Short]()), "smallint", "smallint")

  property("MySqlStore smallint->smallint multiget") =
    withStore(multiPutAndMultiGetStoreTest(_, NonEmpty.Pairing.numerics[Short]()),
      "smallint", "smallint", multiGet = true)

  private def withStore[T](f: MySqlStore => T, kColType: String, vColType: String,
      multiGet: Boolean = false): T = {
    val client = Mysql.client
      .withCredentials("storehaususer", "test1234")
      .withDatabase("storehaus_test")
      .newRichClient("127.0.0.1:3306")
    // these should match mysql setup used in .travis.yml

    val tableName = s"storehaus-mysql-$kColType-$vColType${if (multiGet) "-multiget" else ""}"
    val schema = s"CREATE TEMPORARY TABLE IF NOT EXISTS `$tableName` (`key` $kColType " +
      s"DEFAULT NULL, `value` $vColType DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))
    f(newStore(client, tableName))
  }

  def newStore(client: Client, tableName: String): MySqlStore =
    aggregateCloseable(MySqlStore(client, tableName, "key", "value"))
}
