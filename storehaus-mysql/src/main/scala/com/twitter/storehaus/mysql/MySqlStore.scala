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

import com.twitter.finagle.exp.mysql.{ Client, Result, Parameter }
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.Store
import com.twitter.util.{ Future, Time }

/**
  * @author Ruban Monu
  */

/** Factory for [[com.twitter.storehaus.mysql.MySqlStore]] instances. */
object MySqlStore {
  def apply(client: Client, table: String, kCol: String, vCol: String): MySqlStore =
    new MySqlStore(client, table, kCol, vCol)
}

/**
  * Simple storehaus wrapper over finagle-mysql.
  *
  * Assumes the underlying table's key and value columns are both strings.
  * Supported MySQL column types are: BLOB, TEXT, VARCHAR.
  *
  * The finagle-mysql client is required to set the user, database and create
  * the underlying table schema prior to this class being used.
  *
  * Storehaus-mysql also works with pre-populated MySQL tables, based on the assumption
  * that the key column picked is unique.
  * Any table columns other than the picked key and value columns are ignored during reads and
  * writes.
  *
  * Example usage:
  * {{{
  * import com.twitter.finagle.exp.mysql.Client
  * import com.twitter.storehaus.mysql.MySqlStore
  *
  * val client = Client("localhost:3306", "storehaususer", "test1234", "storehaus_test")
  * val schema = """CREATE TABLE `storehaus-mysql-test` (
  *       `key` varchar(40) DEFAULT NULL,
  *       `value` varchar(100) DEFAULT NULL
  *     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
  * // or, use an existing pre-populated table.
  * client.query(schema).get
  * val store = MySqlStore(client, "storehaus-mysql-test", "key", "value")
  * }}}
  */
class MySqlStore(protected [mysql] val client: Client, table: String, kCol: String, vCol: String)
    extends Store[MySqlValue, MySqlValue] {

  protected val SELECT_SQL = s"SELECT ${g(vCol)} FROM ${g(table)} WHERE ${g(kCol)}=?"
  protected val MULTI_SELECT_SQL_PREFIX =
    s"SELECT ${g(kCol)}, ${g(vCol)} FROM ${g(table)} WHERE ${g(kCol)} IN "

  protected val INSERT_SQL = s"INSERT INTO ${g(table)} (${g(kCol)}, ${g(vCol)}) VALUES (?,?)"
  protected val MULTI_INSERT_SQL_PREFIX =
    s"INSERT INTO ${g(table)} (${g(kCol)}, ${g(vCol)}) VALUES "

  protected val UPDATE_SQL = s"UPDATE ${g(table)} SET ${g(vCol)}=? WHERE ${g(kCol)}=?"

  // update multiple rows together. e.g.
  // UDPATE table SET value = CASE key
  //   WHEN "key1" THEN "value1"
  //   WHEN "key2" THEN "value2"
  // END
  // WHERE key IN ("key1", "key2")
  protected val MULTI_UPDATE_SQL_PREFIX = s"UPDATE ${g(table)} SET ${g(vCol)} = CASE ${g(kCol)} "
  protected val MULTI_UPDATE_SQL_INFIX = s" END WHERE ${g(kCol)} IN "

  protected val DELETE_SQL = s"DELETE FROM ${g(table)} WHERE ${g(kCol)}=?"
  protected val MULTI_DELETE_SQL_PREFIX = s"DELETE FROM ${g(table)} WHERE ${g(kCol)} IN "

  protected val START_TXN_SQL = "START TRANSACTION"
  protected val COMMIT_TXN_SQL = "COMMIT"
  protected val ROLLBACK_TXN_SQL = "ROLLBACK"

  // prepared statements to be reused across gets and puts
  // TODO: should this be non-blocking? this is part of object construction, so maybe not?
  protected val selectStmt = client.prepare(SELECT_SQL)
  protected val insertStmt = client.prepare(INSERT_SQL)
  protected val updateStmt = client.prepare(UPDATE_SQL)
  protected val deleteStmt = client.prepare(DELETE_SQL)

  protected [mysql] def startTransaction : Future[Unit] = client.query(START_TXN_SQL).unit
  protected [mysql] def commitTransaction : Future[Unit] = client.query(COMMIT_TXN_SQL).unit
  protected [mysql] def rollbackTransaction : Future[Unit] = client.query(ROLLBACK_TXN_SQL).unit

  protected [mysql] def executeMultiInsert[K1 <: MySqlValue](kvs: Map[K1, MySqlValue]) = {
    val insertSql = MULTI_INSERT_SQL_PREFIX +
      Stream.continually("(?, ?)").take(kvs.size).mkString(",")
    val insertParams = kvs.map {
      case (k, v) => List(k, v).map(String2MySqlValueInjection.invert(_).getOrElse("").getBytes)
    }.toSeq.flatten.map(Parameter.wrap[Array[Byte]])
    client.prepare(insertSql)(insertParams: _*)
  }

  protected [mysql] def executeMultiUpdate[K1 <: MySqlValue](kvs: Map[K1, MySqlValue]) = {
    val updateSql = MULTI_UPDATE_SQL_PREFIX +
      Stream.continually("WHEN ? THEN ?").take(kvs.size).mkString(" ") +
      MULTI_UPDATE_SQL_INFIX + Stream.continually("?").take(kvs.size).mkString("(", ",", ")")

    val updateParams = kvs.map { kv =>
      (String2MySqlValueInjection.invert(kv._1).getOrElse("").getBytes,
        String2MySqlValueInjection.invert(kv._2).getOrElse("").getBytes)
    }
    // params for "WHEN ? THEN ?"
    val updateCaseParams: Seq[Parameter] =
      updateParams.map { kv => List(kv._1, kv._2) }.toSeq.flatten
        .map(Parameter.wrap[Array[Byte]])
    // params for "IN (?, ?, ?)"
    val updateInParams = updateParams.keys.toSeq.map(Parameter.wrap[Array[Byte]])
    client.prepare(updateSql)(updateCaseParams ++ updateInParams: _*)
  }

  override def get(k: MySqlValue): Future[Option[MySqlValue]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format
    // we assume here the mysql client already has the dbname/schema selected
    val mysqlResult =
      selectStmt.select(String2MySqlValueInjection.invert(k).getOrElse("").getBytes) { row =>
        row(vCol).map { MySqlValue(_)}
      }

    mysqlResult.map { result => result.headOption.flatten }
  }

  override def multiGet[K1 <: MySqlValue](ks: Set[K1]): Map[K1, Future[Option[MySqlValue]]] = {
    if (ks.isEmpty) {
      Map()
    } else {
      // build preparedstatement based on keyset size
      val placeholders = Stream.continually("?").take(ks.size).mkString("(", ",", ")")
      val selectSql = MULTI_SELECT_SQL_PREFIX + placeholders

      val params = ks.map(String2MySqlValueInjection.invert(_).getOrElse("").getBytes).toSeq
        .map(Parameter.wrap[Array[Byte]])
      val mysqlResult =
        client.prepare(selectSql).select(params: _*) { row =>
          (row(kCol).map(MySqlValue(_)), row(vCol).map(MySqlValue(_)))
        }
      FutureOps.liftValues(
        ks,
        mysqlResult.map { rows =>
          rows.toMap.filterKeys(_.isDefined).map { case (optK, optV) => (optK.get, optV) }
        }, { (k: K1) => Future.None }
      )
    }
  }

  protected def set(k: MySqlValue, v: MySqlValue) = doSet(k, v)

  override def put(kv: (MySqlValue, Option[MySqlValue])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => doSet(key, value).unit
      case (key, None) => doDelete(key).unit
    }
  }

  override def multiPut[K1 <: MySqlValue](
      kvs: Map[K1, Option[MySqlValue]]): Map[K1, Future[Unit]] = {
    // batched version of put. the batch is split into insert, update, and delete statements.
    // reduce your batch size if you are hitting mysql packet limit:
    // http://dev.mysql.com/doc/refman/5.1/en/packet-too-large.html
    val putResult = startTransaction.flatMap { t =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).flatMap { result =>
        val existingKeys = result.filter(_._2.isDefined).keySet
        val newKeys = result.filter { _._2.isEmpty }.keySet

        // handle inserts for new keys
        val insertF = newKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            // do not include None values in insert query
            val insertKvs = newKeys.map(k => k -> kvs.getOrElse(k, None)).filter(_._2.isDefined)
              .toMap.mapValues { v => v.get }
            insertKvs.isEmpty match {
              case true => Future.Unit
              case false => executeMultiInsert(insertKvs)
            }
        }

        // handle update and/or delete for existing keys
        val existingKvs = existingKeys.map { k => k -> kvs.getOrElse(k, None) }

        // do not include None values in update query
        val updateKvs = existingKvs.filter(_._2.isDefined)
          .toMap.mapValues { v => v.get }
        lazy val updateF = updateKvs.isEmpty match {
          case true => Future.Unit
          case false => executeMultiUpdate(updateKvs)
        }

        // deletes
        val deleteKeys = existingKvs.filter { _._2.isEmpty }.map { _._1 }
        lazy val deleteF = deleteKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val deleteSql = MULTI_DELETE_SQL_PREFIX +
              Stream.continually("?").take(deleteKeys.size).mkString("(", ",", ")")
            val deleteParams = deleteKeys
              .map { k =>
                Parameter.wrap[Array[Byte]](
                  String2MySqlValueInjection.invert(k).getOrElse("").getBytes)
              }.toSeq
            client.prepare(deleteSql)(deleteParams: _*)
        }

        // sequence the three queries. the inner futures are lazy
        insertF.flatMap { f =>
          updateF.flatMap { f =>
            deleteF.flatMap { f => commitTransaction }
              .handle { case e: Exception => rollbackTransaction.flatMap { throw e } }
          }
        }
      }
    }
    kvs.mapValues { v => putResult.unit }
  }

  override def close(t: Time): Future[Unit] = {
    // close prepared statements before closing the connection
    client.close(t)
  }

  protected def doSet(k: MySqlValue, v: MySqlValue): Future[Result] = {
    // mysql's insert-or-update syntax works only when a primary key is defined:
    // http://dev.mysql.com/doc/refman/5.1/en/insert-on-duplicate.html
    // since we are not guaranteed that, we first check if key exists
    // and insert or update accordingly
    get(k).flatMap {
      case Some(value) =>
        updateStmt(String2MySqlValueInjection.invert(v).getOrElse("").getBytes,
          String2MySqlValueInjection.invert(k).getOrElse("").getBytes)
      case None =>
        insertStmt(String2MySqlValueInjection.invert(k).getOrElse("").getBytes,
          String2MySqlValueInjection.invert(v).getOrElse("").getBytes)
    }
  }

  protected def doDelete(k: MySqlValue): Future[Result] = {
    deleteStmt(String2MySqlValueInjection.invert(k).getOrElse("").getBytes)
  }

  // enclose table or column names in backticks, in case they happen to be sql keywords
  protected def g(s: String): String = s"`$s`"
}
