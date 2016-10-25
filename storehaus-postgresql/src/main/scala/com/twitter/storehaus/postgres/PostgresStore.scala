package com.twitter.storehaus.postgres

import com.twitter.storehaus.Store
import com.twitter.util.{Future, Time}
import roc.postgresql.{Client, Element, Request, Result}

/**
  * Key value storage with PostgreSQL (>9.5) as backend
  *
  * Created by ponkin on 9/24/16.
  */
object PostgresStore {

  def apply[K: Show : Read, V: Show : Read](client: Client, table: String, kCol: String, vCol: String) =
    new PostgresStore[K, V](client, table, kCol, vCol)
}

class PostgresStore[K: Show : Read, V: Show : Read]
(protected[postgres] val client: Client, table: String, kCol: String, vCol: String)
  extends Store[K, V] {

  implicit def toPostgresValue(e: Element): PostgresValue = RocPostgresValue(e)

  private val EMPTY_STRING = ""

  protected val SELECT_SQL_PREFIX =
    s"SELECT $kCol, $vCol FROM $table WHERE $kCol"

  protected val DELETE_SQL_PREFIX =
    s"DELETE FROM $table WHERE $kCol"

  override def get(key: K): Future[Option[V]] = {
    val result = run(selectSQL(List(key)))
    result.map {
      _.headOption.map(col => Read[V].read(col.get(Symbol(vCol))))
    }
  }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    if (ks.isEmpty) {
      Map.empty
    } else {
      val result = run(selectSQL(ks.toList))
      val reqMap = result.map {
        _.map(row => Read[K].read(row.get(Symbol(kCol))) -> Read[V].read(row.get(Symbol(vCol)))).toMap
      }
      ks.foldLeft(Map.empty[K1, Future[Option[V]]]) { (acc, key) =>
        acc + (key -> reqMap.map(_.get(key)))
      }
    }
  }

  private def selectSQL[K1 <: K](ks: List[K1]): String = ks match {
    case Nil => EMPTY_STRING
    case x :: Nil => SELECT_SQL_PREFIX + s" = ${Show[K].show(x)}"
    case res => SELECT_SQL_PREFIX + " IN " + res.map(Show[K].show).mkString("(", ", ", ")")
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    val sql = kv match {
      case (key, Some(value)) => upsertSQL(List((key, value)))
      case (key, None) => deleteSQL(List(key))
    }
    println(sql)
    run(sql).unit
  }

  protected def upsertSQL(values: List[(K, V)]): String = {
    // 'upsert' is available since PostgreSQL 9.5
    // https://wiki.postgresql.org/wiki/UPSERT
    val valueStmt = values.map {
      case (key, value) => s"(${Show[K].show(key)}, ${Show[V].show(value)})"
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
      case x :: Nil => DELETE_SQL_PREFIX + s" = ${Show[K].show(x)};"
      case _ => DELETE_SQL_PREFIX + s" IN ${keys.map(Show[K].show).mkString("(", ", ", ")")};"
    }
  }

  private def multiPutSQL(toSet: List[(K, V)], toDel: List[K]): String = // Do in transaction
    s"""BEGIN;
        |${upsertSQL(toSet)}
        |${deleteSQL(toDel)}
        |END;
        |""".stripMargin

  private def run(sql: String): Future[Result] =
    client.query(Request(sql))

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if (kvs.isEmpty) {
      Map.empty
    } else {
      val sql = doMultiPut(kvs)
      println(sql)
      val result = run(sql).unit
      kvs.mapValues(_ => result)
    }
  }

  private def doMultiPut[K1 <: K](kvs: Map[K1, Option[V]]): String = {
    val (keysToUpsert, toDelete) = kvs.keySet.partition(k => kvs.get(k).exists(_.isDefined))
    val toUpsert = kvs.filterKeys(keysToUpsert.contains).mapValues(_.get).toList
    multiPutSQL(toUpsert, toDelete.toList)
  }

  override def close(t: Time): Future[Unit] = {
    client.close(t)
  }

}

