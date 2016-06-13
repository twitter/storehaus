package com.twitter.storehaus.leveldb

import java.io.File
import java.util

import com.twitter.storehaus.Store
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Future, Await}
import org.iq80.leveldb.Options
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll

import scala.util.Random

/**
 * @author Ben Fradet
 */
object LevelDBStoreProperties extends Properties("LevelDBStore") {

  def putAndGetTest(store: Store[Array[Byte], Array[Byte]],
                    pairs: Gen[List[(Array[Byte], Option[Array[Byte]])]]) =
    forAll(pairs) { examples: List[(Array[Byte], Option[Array[Byte]])] =>
      examples.forall {
        case (k, v) =>
          Await.result(store.put(k, v))
          val found = Await.result(store.get(k))
          found match {
            case Some(a) => util.Arrays.equals(a, v.get)
            case None => found == v
          }
      }
    }

  def multiPutAndGetTest(store: Store[Array[Byte], Array[Byte]],
                         pairs: Gen[List[(Array[Byte], Option[Array[Byte]])]]) =
    forAll(pairs) { examples: List[(Array[Byte], Option[Array[Byte]])] =>
      val examplesMap = examples.toMap
      Await.result(Future.collect(store.multiPut(examplesMap).values.toList))
      val result = store.multiGet(examplesMap.keySet)
        .map { case (key, v) => (key, Await.result(v)) }

      val stringifiedResults = stringifyMap(result)
      val stringifiedExamples = stringifyMap(examplesMap)

      stringifiedResults == stringifiedExamples
    }

  private def stringifyMap(map: Map[Array[Byte], Option[Array[Byte]]])
      :Map[String, Option[String]] = {
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
