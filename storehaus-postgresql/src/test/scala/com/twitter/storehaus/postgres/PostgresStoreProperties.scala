package com.twitter.storehaus.postgres

import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.util.{Await, Future}
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Prop, Properties}
import roc.Postgresql
import roc.postgresql.{Client, Request}

object PostgresStoreProperties extends Properties("PostgresStore") {

  implicit val alphaStrAndBytePair: Gen[(String, Option[Array[Byte]])] = for {
    str <- NonEmpty.alphaStr
    opt <- NonEmpty.byteArrayOpt
  } yield (str, opt)

  implicit val alphaStrAndOptStrPair: Gen[(String, Option[String])] = for {
    str <- NonEmpty.alphaStr
    opt <- NonEmpty.alphaStrOpt
  } yield (str, opt)

  implicit val alphaStrAndBoolOptPair: Gen[(String, Option[Boolean])] = for {
    str <- NonEmpty.alphaStr
    opt <- Gen.oneOf(Some(true), Some(false), None)
  } yield (str, opt)

  // Postgres double precision has 8 digits
  // Need to round
  private def roundDoubleTo8Precision(value: Double): Double =
    Math.round(value * 100000000d) / 100000000d

  // Postgres real has 4 digits
  // Need to round
  private def roundRealTo8Precision(value: Float): Float =
    (Math.round(value * 10000d) / 10000d).toFloat

  val alphaStrPosDoublePair: Gen[(String, Option[Double])] = for {
    str <- NonEmpty.alphaStr
    opt <- Gen.posNum[Double].flatMap(l => Gen.oneOf(Some(roundDoubleTo8Precision(l)), None))
  } yield (str, opt)

  val alphaStrPosFloatPair: Gen[(String, Option[Float])] = for {
    str <- NonEmpty.alphaStr
    opt <- Gen.posNum[Float].flatMap(l => Gen.oneOf(Some(roundRealTo8Precision(l)), None))
  } yield (str, opt)

  implicit val alphaStrAndBytePairList = Gen.listOfN(10, alphaStrAndBytePair)
  implicit val alphaStrAndStrOptPairList = Gen.listOfN(10, alphaStrAndOptStrPair)
  implicit val alphaStrAndLongOptPairList = NonEmpty.Pairing.alphaStrNumerics[Long](10)
  implicit val alphaStrAndIntOptPairList = NonEmpty.Pairing.alphaStrNumerics[Int](10)
  implicit val alphaStrAndShortOptPairList = NonEmpty.Pairing.alphaStrNumerics[Short](10)
  implicit val alphaStrAndFloatOptPairList = Gen.listOfN(10, alphaStrPosDoublePair)
  implicit val alphaStrAndDoubleOptPairList = Gen.listOfN(10, alphaStrPosFloatPair)
  implicit val alphaStrAndBoolOptPairList = Gen.listOfN(10, alphaStrAndBoolOptPair)

  def put[K, V](s: PostgresStore[K, V], pairs: List[(K, Option[V])]) {
    pairs.foreach { case (k, v) =>
      Await.result(s.put((k, v)))
    }
  }

  def multiPut[K, V](s: PostgresStore[K, V], pairs: List[(K, Option[V])]) {
    Await.result(Future.collect(s.multiPut(pairs.toMap).values.toSeq))
  }

  def compareValues[K, V](
                           k: K, expectedOptV: Option[V], foundOptV: Option[V]): Boolean = {
    val isMatch = expectedOptV match {
      case Some(array) if array.isInstanceOf[Array[Byte]] => foundOptV.isDefined && compareArrays(array.asInstanceOf[Array[Byte]], foundOptV.get.asInstanceOf[Array[Byte]])
      case Some(value) => foundOptV.isDefined && foundOptV.get == value
      case None => foundOptV.isEmpty
    }
    if (!isMatch) printErr(k, expectedOptV, foundOptV)
    isMatch
  }

  def compareArrays(a: Array[Byte], b: Array[Byte]): Boolean = a.deep == b.deep

  def printErr[K, V](k: K, expectedOptV: Option[V], foundOptV: Option[V]) {
    println(
      s"""FAILURE: Key "$k" - """ +
        s"expected value ${expectedOptV.get}, but found ${foundOptV.get}")
  }

  def putAndGetStoreTest[K, V](
                                                          store: PostgresStore[K, V]
                                                        )(implicit gen: Gen[List[(K, Option[V])]]): Prop =
    forAll(gen) { examples =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        val foundOptV = Await.result(store.get(k))
        compareValues(k, optV, foundOptV)
      }
    }

  def multiPutAndMultiGetStoreTest[K, V](
                                    store: PostgresStore[K, V]
                                  )(implicit gen: Gen[List[(K, Option[V])]]): Prop =
    forAll(gen) { examples =>
      multiPut(store, examples)
      val data = examples.toMap
      val result = store.multiGet(data.keySet)
      data.forall { case (k, optV) =>
        val foundOptV = result.get(k) match {
          case Some(v) => Await.result(v)
          case None => None
        }
        compareValues(k, optV, foundOptV)
      }
    }
  property("Postgres text->bytea") =
    withStore(putAndGetStoreTest[String, Array[Byte]], "text", "bytea")

  property("Postgres text->text") =
    withStore(putAndGetStoreTest[String, String], "text", "text")

  property("Postgres text->bigint") =
    withStore(putAndGetStoreTest[String, Long], "text", "bigint")

  property("Postgres text->integer") =
    withStore(putAndGetStoreTest[String, Int], "text", "integer")

  property("Postgres text->smallint") =
    withStore(putAndGetStoreTest[String, Short], "text", "smallint")

  property("Postgres text->real") =
    withStore(putAndGetStoreTest[String, Float], "text", "real")

  property("Postgres text->double precision") =
    withStore(putAndGetStoreTest[String, Double], "text", "double precision")

  property("Postgres text->bool") =
    withStore(putAndGetStoreTest[String, Boolean], "text", "bool")

  property("Postgres text->text multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, String], "text", "text")

  property("Postgres text->bytea multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Array[Byte]], "text", "bytea")

  property("Postgres text->bigint multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Long], "text", "bigint", multiGet = true)

  property("Postgres text->integer multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Int], "text", "integer", multiGet = true)

  property("Postgres text->smallint multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Short], "text", "smallint", multiGet = true)

  property("Postgres text->real multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Float], "text", "real", multiGet = true)

  property("Postgres text->double precision multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Double], "text", "double precision", multiGet = true)

  property("Postgres text->bool multiget") =
    withStore(multiPutAndMultiGetStoreTest[String, Boolean], "text", "bool", multiGet = true)

  private def withStore[K: Show : Read, V: Show : Read](
                                                         f: PostgresStore[K, V] => Prop,
                                                         kColType: String,
                                                         vColType: String,
                                                         multiGet: Boolean = false): Prop = {
    val client = Postgresql.client
      .withUserAndPasswd("test", "test")
      .withDatabase("test")
      .newRichClient("inet!localhost:5432")
    val tableName = s"storehaus_postgres_${escapeName(kColType)}_${escapeName(vColType)}${if (multiGet) "_multiget" else ""}"
    val schema =
      s"""CREATE TEMPORARY TABLE IF NOT EXISTS $tableName (
          |key $kColType PRIMARY KEY,
          |value $vColType DEFAULT NULL
          |);""".stripMargin
    Await.result(client.query(Request(schema)))
    f(newStore(client, tableName))
  }

  private def escapeName(name: String): String = name.replaceAll("\\s", "")

  def newStore[K: Show : Read, V: Show : Read](client: Client, tableName: String): PostgresStore[K, V] =
    PostgresStore[K, V](client, tableName, "key", "value")
}

