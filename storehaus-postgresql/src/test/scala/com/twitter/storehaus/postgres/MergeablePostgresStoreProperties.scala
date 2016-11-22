package com.twitter.storehaus.postgres

import com.twitter.finagle.postgres.Client
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.util.{Await, Future}

import org.scalacheck.{Gen, Prop, Arbitrary, Properties}
import org.scalacheck.Gen._
import org.scalacheck.Prop._

object MergeablePostgresStoreProperties extends Properties("MergeablePostgresStore") {

  /** Generator for pairings of numerics */
  def numericPair[T : Numeric : Choose]: Gen[(T, T)] = for {
    str <- Gen.posNum[T]
    num <- Gen.posNum[T]
  } yield (str, num)

  /** Generator for pairings of text*/
  def textPair: Gen[(String, String)] = for {
    str1 <- NonEmpty.alphaStr.suchThat(!_.isEmpty)
    str2 <- NonEmpty.alphaStr
  } yield (str1, str2)

  implicit val alphaStrAndIntPairList: Gen[List[(Int, Int)]] = Gen.listOfN(10, numericPair[Int])
  implicit val alphaStrAndtextPairList: Gen[List[(String, String)]] = Gen.listOfN(10, textPair)

  property("MergeablePostgres integer->integer") =
    withStore(mergeTest[Int, Int], "integer", "integer")

  property("MergeablePostgres text->text") =
    withStore(mergeTest[String, String], "text", "text")

  def merge[K, V](s: MergeablePostgresStore[K, V], kvs: Map[K, V]) = {
    val result = s.multiMerge(kvs)
    Await.result(Future.collect(result.values.toList).unit)
  }

  def multiGet[K, V](s: MergeablePostgresStore[K, V], keys: Set[K]) = {
    val result = s.multiGet(keys)
    result.mapValues( future => Await.result(future) )
          .filter( _._2.isDefined )
          .mapValues( _.get )
  }

  def mergeTest[K, V](
                      store: MergeablePostgresStore[K, V])(
                      implicit gen: Gen[List[(K, V)]],
                      semigroup: Semigroup[V]): Prop =
    forAll(gen) { examples =>
      val examplesMap = examples.toMap
      val previousValues = multiGet(store, examplesMap.keySet)
      merge(store, examplesMap)
      examplesMap.map { case (k, tv) =>
        val Some(v) = Await.result(store.get(k))
        val mustBeVal = previousValues.get(k) match {
          case Some(pv) => semigroup.plus(pv, tv)
          case None => tv
        }
        if(mustBeVal != v) println(s"$mustBeVal -> $v")
        mustBeVal ==  v
      }.forall( _ == true )
    }


  private def withStore[K, V](
                              f: MergeablePostgresStore[K, V] => Prop,
                              kColType: String,
                              vColType: String)(
                              implicit kInj: PostgresValueConverter[K],
                              vInj: PostgresValueConverter[V],
                              semigroup: Semigroup[V]): Prop = {
    val client = Client(
      host = "localhost:5432",
      username = "postgres",
      password = Some(""),
      database = "test")
    val tableName = s"storehaus_mergeable_postgres_${kColType}_${vColType}"
    val schema =
      s"""CREATE TEMPORARY TABLE IF NOT EXISTS $tableName (
          |key $kColType PRIMARY KEY,
          |value $vColType DEFAULT NULL
          |);""".stripMargin
    Await.result(client.query(schema))
    f(newStore(client, tableName))
  }

  private def newStore[K, V]( 
                              client: Client,
                              tableName: String)(
                              implicit semigroup: Semigroup[V],
                              kInj: PostgresValueConverter[K],
                              vInj: PostgresValueConverter[V]) =
        new MergeablePostgresStore[K, V](new PostgresStore(client, tableName, "key", "value"))
 
}
