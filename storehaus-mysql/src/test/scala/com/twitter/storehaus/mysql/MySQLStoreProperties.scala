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

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

object MySQLStoreProperties extends Properties("MySQLStore") {

  // used to generate arbitrary pairs of types we want to test
  def validPairs[T: Arbitrary] = Arbitrary.arbitrary[List[(T, Option[T])]] suchThat(!_.isEmpty)

  def put(s: MySQLStore, pairs: List[(String, Option[String])]) {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v match {
          case Some(value) => Some(ChannelBuffers.copiedBuffer(value, UTF_8))
          case None => None
        }
      )))
    }
  }

  def putAndGetStoreTest(store: MySQLStore, pairs: Gen[List[(Any, Option[Any])]] = validPairs[String]) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = examples.map { case (k, v) =>
          (k.toString, v match {
            case Some(d) => Some(d.toString)
            case None => None
          })
        }
      put(store, stringified)
      stringified.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def putAndMultiGetStoreTest(store: MySQLStore, pairs: Gen[List[(Any, Option[Any])]] = validPairs[String]) =
    forAll(pairs) { (examples: List[(Any, Option[Any])]) =>
      val stringified = examples.map { case (k, v) =>
          (k.toString, v match {
            case Some(d) => Some(d.toString)
            case None => None
          })
        }
      put(store, stringified)
      val data = stringified.toMap
      val result = store.multiGet(data.keySet)
      data.forall { case (k, optV) =>
        // result.get(k) returns Option[Future[Option[ChannelBuffer]]]
        val foundOptV = result.get(k) match { case Some(v) => Await.result(result.get(k).get) ; case None => None }
        compareValues(k, optV, foundOptV)
      }
    }

  def compareValues(k: String, expectedOptV: Option[String], foundOptV: Option[ChannelBuffer]) = {
    val isMatch = expectedOptV match {
      case Some(value) => !foundOptV.isEmpty && foundOptV.get.toString(UTF_8) == value 
      case None => foundOptV.isEmpty
    }
    if (!isMatch) printErr(k, expectedOptV, foundOptV)
    isMatch
  }

  def printErr(k: String, expectedOptV: Option[String], foundOptV: Option[ChannelBuffer]) {
    val found = if (foundOptV.isEmpty) { foundOptV } else { "Some("+foundOptV.get.toString(UTF_8)+")" }
    println("FAILURE: Key \""+k+"\" - expected value "+expectedOptV+", but found "+found)
  }

  property("MySQLStore text->text") =
    withStore(putAndGetStoreTest(_), "text", "text")

  property("MySQLStore blob->blob") =
    withStore(putAndGetStoreTest(_), "blob", "blob")

  property("MySQLStore text->blob") =
    withStore(putAndGetStoreTest(_), "text", "blob")

  property("MySQLStore text->text multiget") =
    withStore(putAndMultiGetStoreTest(_), "text", "text", true)

  property("MySQLStore blob->blob multiget") =
    withStore(putAndMultiGetStoreTest(_), "blob", "blob", true)

  property("MySQLStore text->blob multiget") =
    withStore(putAndMultiGetStoreTest(_), "text", "blob", true)

  property("MySQLStore int->int") =
    withStore(putAndGetStoreTest(_, validPairs[Int]), "int", "int")

  property("MySQLStore int->int multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Int]), "int", "int", true)

  property("MySQLStore bigint->bigint") =
    withStore(putAndGetStoreTest(_, validPairs[Long]), "bigint", "bigint")

  property("MySQLStore bigint->bigint multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Long]), "bigint", "bigint", true)

  property("MySQLStore smallint->smallint") =
    withStore(putAndGetStoreTest(_, validPairs[Short]), "smallint", "smallint")

  property("MySQLStore smallint->smallint multiget") =
    withStore(putAndMultiGetStoreTest(_, validPairs[Short]), "smallint", "smallint", true)

  private def withStore[T](f: MySQLStore => T, kColType: String, vColType: String, multiGet: Boolean = false): T = {
    val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test", Level.WARNING)
    // these should match mysql setup used in .travis.yml

    val tableName = "storehaus-mysql-"+kColType+"-"+vColType + ( if (multiGet) { "-multiget" } else { "" } )
    val schema = "CREATE TEMPORARY TABLE IF NOT EXISTS `"+tableName+"` (`key` "+kColType+" DEFAULT NULL, `value` "+vColType+" DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    Await.result(client.query(schema))

    val store = MySQLStore(client, tableName, "key", "value")
    val result = f(store)
    store.close
    result
  }
}
