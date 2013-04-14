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

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object MySQLStoreProperties extends Properties("MySQLStore") {

  val validPairs = Arbitrary.arbitrary[List[(String, Option[String])]] suchThat {
    case Nil => false
    case _ => true
    /*
    case xs => xs.forall {
      case (k, Some(s)) => !k.isEmpty && !s.isEmpty
      case (k, None) => !k.isEmpty
    }
    */
  }

  def put(s: MySQLStore, pairs: List[(String, Option[String])]) {
    pairs.foreach { case (k, v) =>
      s.put((k, v match {
          case Some(value) => Some(ChannelBuffers.copiedBuffer(value, UTF_8))
          case None => None
        }
      )).get
    }
  }

  def putAndGetStoreTest(store: MySQLStore) =
    forAll(validPairs) { (examples: List[(String, Option[String])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        store.get(k).get match {
          case Some(value) => (value.toString(UTF_8) == optV.get)
          case None => optV.isEmpty
        }
      }
    }

  def putAndMultiGetStoreTest(store: MySQLStore) =
    forAll(validPairs) { (examples: List[(String, Option[String])]) =>
      put(store, examples)
      val data = examples.toMap
      val result = store.multiGet(data.keySet)
      data.forall { case (k, optV) =>
        optV match {
          case Some(value) => !result.get(k).isEmpty && (result.get(k).get.get.get.toString(UTF_8) == optV.get)
          case None => result.get(k).isEmpty
        }
      }
    }

  def storeTest(store: MySQLStore) =
    putAndGetStoreTest(store) && putAndMultiGetStoreTest(store)

  property("MySQLStore text->text") =
    withStore(storeTest(_), "text", "text")

  property("MySQLStore blob->blob") =
    withStore(storeTest(_), "blob", "blob")

  property("MySQLStore text->blob") =
    withStore(storeTest(_), "text", "blob")

  private def withStore[T](f: MySQLStore => T, kColType: String, vColType: String): T = {
    val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test")
    // these should match mysql setup used in .travis.yml

    val tableName = "storehaus-mysql-"+kColType+"-"+vColType
    val schema = "CREATE TEMPORARY TABLE IF NOT EXISTS `"+tableName+"` (`key` "+kColType+" DEFAULT NULL, `value` "+vColType+" DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    client.query(schema).get

    val store = MySQLStore(client, tableName, "key", "value")
    val result = f(store)
    store.close
    result
  }
}
