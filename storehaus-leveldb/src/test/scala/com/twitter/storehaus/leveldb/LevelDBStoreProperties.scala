/*
 * Copyright 2015 Twitter Inc.
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

package com.twitter.storehaus.leveldb

import java.io.File
import java.util

import com.twitter.storehaus.Store
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Future, Await}
import org.iq80.leveldb.Options
import org.scalacheck.{Prop, Gen, Properties}
import org.scalacheck.Prop.forAll

import scala.util.Random

/**
 * @author Ben Fradet
 */
object LevelDBStoreProperties extends Properties("LevelDBStore") {

  def putAndGetTest(store: Store[Array[Byte], Array[Byte]],
      pairs: Gen[List[(Array[Byte], Option[Array[Byte]])]]): Prop =
    forAll(pairs) { examples: List[(Array[Byte], Option[Array[Byte]])] =>
      examples.forall {
        case (k, v) =>
          Await.result(store.put((k, v)))
          val found = Await.result(store.get(k))
          found match {
            case Some(a) => util.Arrays.equals(a, v.get)
            case None => found == v
          }
      }
    }

  def multiPutAndGetTest(store: Store[Array[Byte], Array[Byte]],
      pairs: Gen[List[(Array[Byte], Option[Array[Byte]])]]): Prop =
    forAll(pairs) { examples: List[(Array[Byte], Option[Array[Byte]])] =>
      val examplesMap = examples.toMap
      Await.result(Future.collect(store.multiPut(examplesMap).values.toList))
      val result = store.multiGet(examplesMap.keySet)
        .map { case (key, v) => (key, Await.result(v)) }

      val stringifiedResults = stringifyMap(result)
      val stringifiedExamples = stringifyMap(examplesMap)

      stringifiedResults == stringifiedExamples
    }

  private def stringifyMap(
      map: Map[Array[Byte], Option[Array[Byte]]]): Map[String, Option[String]] = {
    map.map {
      case (k, Some(v)) => (new String(k, "UTF-8"),
        Some(new String(v, "UTF-8")))
      case (k, None) => (new String(k, "UTF-8"), None)
    }
  }

  property("LevelDB[Array[Byte], Array[Byte]] single") = {
    val dir = new File(System.getProperty("java.io.tmpdir"),
      "leveldb-test-" + new Random().nextInt(Int.MaxValue))
    dir.mkdirs()
    val store = new LevelDBStore(dir, new Options(), 2)
    putAndGetTest(store, NonEmpty.Pairing.byteArrays())
  }

  property("LevelDB[Array[Byte], Array[Byte] multi") = {
    val dir = new File(System.getProperty("java.io.tmpdir"),
      "leveldb-test-multi-" + new Random().nextInt(Int.MaxValue))
    dir.mkdirs()
    val store = new LevelDBStore(dir, new Options(), 1)
    multiPutAndGetTest(store, NonEmpty.Pairing.byteArrays())
  }
}
