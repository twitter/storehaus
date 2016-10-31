package com.twitter.storehaus.postgres

import com.twitter.finagle.postgres.Client
import com.twitter.finagle.postgres.values.Value
import com.twitter.storehaus.Store
import com.twitter.util.{Future, Time}

import scalaz._

import scala.util.{Failure, Success}

/**
  * Key value storage with PostgreSQL (>9.5) as backend
  *
  * @author Alexey Ponkin
  */
object PostgresStore {

  def apply[K, V]
  (client: Client, table: String, kCol: String, vCol: String)
  (implicit kInj: PostgresValueConverter[K], vInj: PostgresValueConverter[V]) =
    new PostgresStore[K, V](client, table, kCol, vCol)
}

class PostgresStore[K, V]
(protected[postgres] val client: Client, table: String, kCol: String, vCol: String)
(implicit kInj: PostgresValueConverter[K], vInj: PostgresValueConverter[V])
  extends Store[K, V] {

  import Scalaz._

  private val toV: PostgresValue => V = vInj.invert(_) match {
    case Success(v) => v
    case Failure(e) => throw IllegalConversionException(e.getMessage)
  }

  private val toK: PostgresValue => K = kInj.invert(_) match {
    case Success(k) => k
    case Failure(e) => throw IllegalConversionException(e.getMessage)
  }

  implicit def toPostgresValue(e: Value[Any]): PostgresValue = Column(e)

  private val EMPTY_STRING = ""

  private val SELECT_SQL_PREFIX =
    s"SELECT $kCol, $vCol FROM $table WHERE $kCol"

  private val DELETE_SQL_PREFIX =
    s"DELETE FROM $table WHERE $kCol"

  override def get(key: K): Future[Option[V]] = {
    doGet(key).run(client)
  }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val result = doGet(ks).run(client)
    ks.iterator.map { key =>
      (key, result.map(_.get(key)))
    }.toMap
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    val query = kv match {
      case (key, Some(value)) => doUpsert(List((key, value)))
      case (key, None) => doDelete(List(key))
    }
    query.run(client).unit
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if (kvs.isEmpty) {
      Map.empty
    } else {
      val result = doMultiput(kvs)(client)
      kvs.mapValues(_ => result)
    }
  }

  private def selectSQL[K1 <: K](ks: List[K1]): String = ks match {
    case Nil => EMPTY_STRING
    case x :: Nil => SELECT_SQL_PREFIX + s" = ${toEscString(x)}"
    case res => SELECT_SQL_PREFIX + " IN " + res.map(toEscString).mkString("(", ", ", ")")
  }

  private def upsertSQL(values: List[(K, V)]): String = {
    // 'upsert' is available since PostgreSQL 9.5
    // https://wiki.postgresql.org/wiki/UPSERT
    val valueStmt = values.map {
      case (key, value) => s"(${toEscString(key)}, ${toEscString(value)})"
    }.mkString(", ")
    if (values.nonEmpty) {
      s"""INSERT INTO $table($kCol, $vCol) VALUES $valueStmt
          |ON CONFLICT ($kCol) DO UPDATE SET $vCol = EXCLUDED.$vCol;
          |""".stripMargin
    } else {
      EMPTY_STRING
    }
  }

  private def deleteSQL(keys: List[K]): String = {
    keys match {
      case Nil => EMPTY_STRING
      case x :: Nil => DELETE_SQL_PREFIX + s" = ${toEscString(x)};"
      case _ => DELETE_SQL_PREFIX + s" IN ${keys.map(toEscString).mkString("(", ", ", ")")};"
    }
  }

  protected[postgres] def doGet(k: K): DBRequest[Option[V]] = Kleisli {
    _.prepareAndQuery(selectSQL(List(k)))( row => toV(row.get(1)) ).map( v => v.headOption )
  }

  protected[postgres] def doGet[K1 <: K](ks: Set[K1]): DBRequest[Map[K1, V]] = Kleisli {
    if (ks.isEmpty) {
      _ => Future(Map.empty)
    } else {
      val query = selectSQL(ks.toList)
      _.prepareAndQuery(query){ row =>
        (toK(row.get(0)), toV(row.get(1)))
      }.map( _.toMap )
    }
  }

  protected[postgres] def doUpsert[K1 <: K](values: List[(K1, V)]): DBRequest[Unit] =
    Kleisli{ _.query(upsertSQL(values)).unit }

  protected[postgres] def doDelete[K1 <: K](values: List[K1]): DBRequest[Unit] =
    Kleisli{ _.query(deleteSQL(values)).unit }

  protected[postgres] def doMultiput[K1 <: K](kvs: Map[K1, Option[V]]): DBRequest[Unit] = {
    val (keysToUpsert, toDelete) = kvs.keySet.partition(k => kvs.get(k).exists(_.isDefined))
    val toUpsert = kvs.filterKeys(keysToUpsert.contains).mapValues(_.get).toList
    Kleisli{
      _.inTransaction(
        for{
          _ <- doUpsert(toUpsert)
          _ <- doDelete(toDelete.toList)
        } yield () 
      )
    }
  }

  override def close(t: Time): Future[Unit] = client.close()

}

