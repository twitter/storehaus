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

import com.twitter.storehaus.mysql.compat.{Client, Result, Parameter}
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.Store
import com.twitter.util.{Future, Time}

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
  protected [mysql] def commitOrRollback(): Future[Unit] =
    commitTransaction
      .handle { case e: Throwable => rollbackTransaction.flatMap(_ => Future.exception(e)) }

  protected [mysql] def executeMultiInsert[K1 <: MySqlValue](
      kvs: Map[K1, MySqlValue]): Future[Result] = {
    val insertSql = MULTI_INSERT_SQL_PREFIX + buildPlaceholders("(?, ?)", kvs.size, ",")

    val insertParams = kvs.flatMap {
      case (k, v) => List(k, v)
        .map(e => String2MySqlValueInjection.invert(e).map(s => Parameter.wrap(s.getBytes)))
    }.toSeq
    toTwitterFuture(insertParams).flatMap(ps => client.prepare(insertSql)(ps: _*))
  }

  protected [mysql] def executeMultiUpdate[K1 <: MySqlValue](
      kvs: Map[K1, MySqlValue]): Future[Result] = {
    val updateSql = MULTI_UPDATE_SQL_PREFIX + buildPlaceholders("WHEN ? THEN ?", kvs.size, " ") +
      MULTI_UPDATE_SQL_INFIX +  buildPlaceholders("?", kvs.size, ",", "(", ")")

    val updateParams = kvs.map { case (k, v) =>
      (String2MySqlValueInjection.invert(k).map(_.getBytes),
        String2MySqlValueInjection.invert(v).map(_.getBytes))
    }
    // params for "WHEN ? THEN ?"
    val updateCaseParams =
      updateParams.flatMap { case (k, v) => List(k, v) }.toSeq
        .map(_.map(Parameter.wrap[Array[Byte]]))
    // params for "IN (?, ?, ?)"
    val updateInParams = updateParams.keys.toSeq.map(_.map(Parameter.wrap[Array[Byte]]))
    toTwitterFuture(updateCaseParams ++ updateInParams)
      .flatMap(ps => client.prepare(updateSql)(ps: _*))
  }

  override def get(k: MySqlValue): Future[Option[MySqlValue]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format
    // we assume here the mysql client already has the dbname/schema selected
    val mysqlResult =
      toTwitterFuture(String2MySqlValueInjection.invert(k)).flatMap { str =>
        selectStmt.select(str.getBytes) { row => row(vCol).map(MySqlValue(_)) }
      }

    mysqlResult.map(_.headOption.flatten)
  }

  override def multiGet[K1 <: MySqlValue](ks: Set[K1]): Map[K1, Future[Option[MySqlValue]]] = {
    if (ks.isEmpty) {
      Map.empty
    } else {
      // build preparedstatement based on keyset size
      val selectSql = MULTI_SELECT_SQL_PREFIX + buildPlaceholders("?", ks.size, ",", "(", ")")

      val params = toTwitterFuture(
        ks.map(String2MySqlValueInjection.invert(_).map(s => Parameter.wrap(s.getBytes))).toSeq)
      val mysqlResult = params.flatMap { ps =>
        client.prepare(selectSql).select(ps: _*) { row =>
          (row(kCol).map(MySqlValue(_)), row(vCol).map(MySqlValue(_)))
        }
      }
      FutureOps.liftValues(
        ks,
        mysqlResult.map(rows => rows.iterator.collect { case (Some(k), v) => (k, v) }.toMap),
        (k: K1) => Future.None
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
        val newKeys = result.filter(_._2.isEmpty).keySet

        // handle inserts for new keys
        val insertF =
          if (newKeys.isEmpty) {
            Future.Unit
          } else {
            // do not include None values in insert query
            val insertKvs = newKeys.iterator
              .collect { case k if kvs.contains(k) && kvs(k).isDefined => k -> kvs(k) }
              .toMap.mapValues(_.get)
            if (insertKvs.isEmpty) Future.Unit
            else executeMultiInsert(insertKvs).unit
          }

        // handle update and/or delete for existing keys
        val existingKvs = existingKeys.map(k => k -> kvs.getOrElse(k, None))

        // do not include None values in update query
        val updateKvs = existingKvs.iterator.collect { case (k, Some(v)) => (k, v) }.toMap
        lazy val updateF =
          if (updateKvs.isEmpty) Future.Unit
          else executeMultiUpdate(updateKvs).unit

        // deletes
        val deleteKeys = existingKvs.iterator.collect { case (k, None) => k }.toSeq
        lazy val deleteF =
          if (deleteKeys.isEmpty) {
            Future.Unit
          } else {
            val deleteSql = MULTI_DELETE_SQL_PREFIX +
              buildPlaceholders("?", deleteKeys.size, ",", "(", ")")
            val deleteParams = deleteKeys
              .map(String2MySqlValueInjection.invert(_).map(s => Parameter.wrap(s.getBytes)))
            toTwitterFuture(deleteParams).flatMap(ps => client.prepare(deleteSql)(ps: _*).unit)
          }

        // sequence the three queries. the inner futures are lazy
        for {
          _ <- insertF
          _ <- updateF
          _ <- deleteF
          _ <- commitOrRollback()
        } yield ()
      }
    }
    kvs.mapValues(v => putResult)
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
        toTwitterFuture(String2MySqlValueInjection.invert(v)).flatMap { vStr =>
          toTwitterFuture(String2MySqlValueInjection.invert(k)).flatMap { kStr =>
            updateStmt(Parameter.wrap(vStr.getBytes), Parameter.wrap(kStr.getBytes))
          }
        }
      case None =>
        toTwitterFuture(String2MySqlValueInjection.invert(v)).flatMap { vStr =>
          toTwitterFuture(String2MySqlValueInjection.invert(k)).flatMap { kStr =>
            insertStmt(Parameter.wrap(kStr.getBytes), Parameter.wrap(vStr.getBytes))
          }
        }
    }
  }

  protected def doDelete(k: MySqlValue): Future[Result] =
    toTwitterFuture(String2MySqlValueInjection.invert(k))
      .flatMap(kStr => deleteStmt(Parameter.wrap(kStr.getBytes)))

  // enclose table or column names in backticks, in case they happen to be sql keywords
  protected def g(s: String): String = s"`$s`"

  private def buildPlaceholders(continually: String, size: Int,
      sep: String, start: String = "", end: String = ""): String =
    Iterator.continually(continually).take(size).mkString(start, sep, end)
}
