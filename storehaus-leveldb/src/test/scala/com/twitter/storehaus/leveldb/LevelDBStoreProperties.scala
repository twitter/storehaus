package com.twitter.storehaus.leveldb

import java.io.File
import java.util

import com.twitter.storehaus.Store
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.Await
import org.iq80.leveldb.Options
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop.forAll

import scala.util.Random

/**
 * @author Ben Fradet
 */
object LevelDBStoreProperties extends Properties("LevelDBStore") {

  def putAndGetStoreTest(store: Store[Array[Byte], Array[Byte]],
                         pairs: Gen[List[(Array[Byte], Option[Array[Byte]])]]) =
    forAll(pairs) { examples: List[(Array[Byte], Option[Array[Byte]])] =>
      examples.forall {
        case (k, v) => {
          Await.result(store.put(k, v))
          val found = Await.result(store.get(k))
          found match {
            case Some(a) => util.Arrays.equals(a, v.get)
            case None => found == v
          }
        }
      }
    }

  property("LevelDB[Array[Byte], Array[Byte]]") = {
    val dir = new File(System.getProperty("java.io.tmpdir"),
      "leveldb-test-" + new Random().nextInt(Int.MaxValue))
    dir.mkdirs()
    val store = new LevelDBStore(dir, new Options().createIfMissing(true), 2)
    putAndGetStoreTest(store, NonEmpty.Pairing.byteArrays())
  }
}
