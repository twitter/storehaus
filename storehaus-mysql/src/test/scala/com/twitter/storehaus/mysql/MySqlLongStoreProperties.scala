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

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.Client
import com.twitter.storehaus.testing.SelfAggregatingCloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Await, Future}
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.Prop.forAll



object MySqlLongStoreProperties extends Properties("MySqlLongStore")
  with SelfAggregatingCloseableCleanup[MySqlLongStore] {

  private[this] class PropertyCached(ps: PropertySpecifier) {
    def update(propName: String, p: Prop) = {
      ps(propName) = p
    }
  }

  /**
   * Property specification is used by name since scalacheck 1.13.4, which breaks
   * tests here. This simulates the old behavior.
   */
  private[this] val propertyCached = new PropertyCached(property)

  def put(s: MySqlLongStore, pairs: List[(MySqlValue, Option[Long])]): Unit = {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def merge(s: MySqlLongStore, kvs: Map[MySqlValue, Long]): Map[MySqlValue, Future[Option[Long]]] =
    s.multiMerge(kvs)

  def putAndGetStoreTest(
    store: MySqlLongStore,
    pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)
  ): Prop =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      val kvs = examples.map { case (k, v) =>
        // lowercase the keys because mysql queries are case-insensitive
        (String2MySqlValueInjection(k.toLowerCase), v)
      }
      put(store, kvs)
      kvs.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def multiMergeStoreTest(
    store: MySqlLongStore,
    pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)
  ): Prop =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      val kvs = examples.map { case (k, v) =>
        // lowercase the keys because mysql queries are case-insensitive
        (String2MySqlValueInjection(k.toLowerCase), v)
      }
      put(store, kvs)
      // increment values
      merge(store, kvs.map { case (k, v) => (k, 1L) }.toMap).forall { case (k, futureOptV) =>
        val foundOptV = Await.result(futureOptV)
        // verify old values are returned
        compareValues(k, kvs.toMap.get(k).get, foundOptV)
      }
      // now verify incremented values are returned
      kvs.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV.map { _ + 1L }.orElse(Some(1L)), foundOptV)
      }
    }

  def compareValues(k: MySqlValue, expectedOptV: Option[Long], foundOptV: Option[Long]): Boolean = {
    val isMatch = expectedOptV match {
      case Some(value) => foundOptV.isDefined && foundOptV.get == value
      case None => foundOptV.isEmpty
    }
    if (!isMatch) {
      println(
        s"""FAILURE: Key "${String2MySqlValueInjection.invert(k)}" - """" +
          s"expected value $expectedOptV, but found $foundOptV")
    }
    isMatch
  }


  propertyCached("MySqlLongStore put and get") =
    withStore(putAndGetStoreTest(_), "text", "bigint")

  propertyCached("MySqlLongStore multiMerge") =
    withStore(multiMergeStoreTest(_), "text", "bigint", merge = true)

  private def withStore[T](f: MySqlLongStore => T, kColType: String, vColType: String,
      merge: Boolean = false): T = {
    val client = Mysql.client
      .withCredentials("storehaususer", "test1234")
      .withDatabase("storehaus_test")
      .newRichClient("127.0.0.1:3306")
    // these should match mysql setup used in .travis.yml

    val tableName = s"storehaus-mysql-long-$kColType-$vColType${if (merge) "-merge" else ""}"
    val schema = s"CREATE TEMPORARY TABLE IF NOT EXISTS `$tableName` (`key` $kColType DEFAULT " +
      s"NULL, `value` $vColType DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))

    f(newStore(client, tableName))
  }

  def newStore(client: Client, tableName: String): MySqlLongStore =
    aggregateCloseable(MySqlLongStore(MySqlStore(client, tableName, "key", "value")))

}

