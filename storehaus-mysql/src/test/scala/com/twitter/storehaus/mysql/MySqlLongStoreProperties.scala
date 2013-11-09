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

object MySqlLongStoreProperties extends Properties("MySqlLongStore")
  with SelfAggregatingCloseableCleanup[MySqlLongStore] {

  def put(s: MySqlLongStore, pairs: List[(MySqlValue, Option[Long])]) = {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def merge(s: MySqlLongStore, kvs: Map[MySqlValue, Long]) = s.multiMerge(kvs)

  def putAndGetStoreTest(store: MySqlLongStore,
      pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)) =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      val kvs = examples.map { case (k, v) =>
        // lowercase the keys because mysql queries are case-insensitive
        (MySqlStringInjection.invert(k.toLowerCase).get, v)
      }
      put(store, kvs)
      kvs.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def mergeStoreTest(store: MySqlLongStore,
      pairs: Gen[List[(String, Option[Long])]] = NonEmpty.Pairing.alphaStrNumerics[Long](10)) =
    forAll(pairs) { (examples: List[(String, Option[Long])]) =>
      val kvs = examples.map { case (k, v) =>
        // lowercase the keys because mysql queries are case-insensitive
        (MySqlStringInjection.invert(k.toLowerCase).get, v)
      }
      put(store, kvs)
      // increment values
      merge(store, kvs.map { case (k, v) => (k, 1l) }.toMap).forall { case (k, futureOptV) =>
        val foundOptV = Await.result(futureOptV)
        // verify old values are returned
        compareValues(k, kvs.toMap.get(k).get, foundOptV)
      }
      // now verify incremented values are returned
      kvs.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV.map { _ + 1l }.orElse(Some(1l)), foundOptV)
      }
    }

  def compareValues(k: MySqlValue, expectedOptV: Option[Long], foundOptV: Option[Long]) = {
    val isMatch = expectedOptV match {
      case Some(value) => !foundOptV.isEmpty && foundOptV.get == value
      case None => foundOptV.isEmpty
    }
    if (!isMatch)
      println("FAILURE: Key \"" + MySqlStringInjection(k) + "\" - expected value " + expectedOptV + ", but found " + foundOptV)
    isMatch
  }

  property("MySqlLongStore put and get") =
    withStore(putAndGetStoreTest(_), "text", "bigint")

  property("MySqlLongStore merge") =
    withStore(putAndGetStoreTest(_), "text", "bigint", true)

  private def withStore[T](f: MySqlLongStore => T, kColType: String, vColType: String, merge: Boolean = false): T = {
    val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test", Level.WARNING)
    // these should match mysql setup used in .travis.yml

    val tableName = "storehaus-mysql-long-"+kColType+"-"+vColType + ( if (merge) { "-merge" } else { "" } )
    val schema = "CREATE TEMPORARY TABLE IF NOT EXISTS `"+tableName+"` (`key` "+kColType+" DEFAULT NULL, `value` "+vColType+" DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))

    f(newStore(client, tableName))
  }

  def newStore(client: Client, tableName: String) =
    aggregateCloseable(MySqlLongStore(MySqlStore(client, tableName, "key", "value")))

}

