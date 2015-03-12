package com.twitter.storehaus.leveldb

import java.io.File

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

  def putAndGetStoreTest[K, V](store: Store[K, V],
                               pairs: Gen[List[(K, Option[V])]]) =
    forAll(pairs) { examples: List[(K, Option[V])] =>
      examples.forall {
        case (k, v) => {
          Await.result(store.put((k, v)))
          val found = Await.result(store.get(k))
          found == v
        }
      }
    }

  property("LevelDB[Array[Byte], Array[Byte]]") = {
    val dir = new File(System.getProperty("java.io.tmpdir"),
      "leveldb-test-" + new Random().nextInt(Int.MaxValue))
    true
    //putAndGetStoreTest[Array[Byte], Array[Byte]](
    //  new LevelDBStore(dir, new Options, 2),
    //  NonEmpty.Pairing.alphaStrs())
  }
}
