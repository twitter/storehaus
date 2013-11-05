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

import com.twitter.finagle.exp.mysql.{ Client, PreparedStatement, Result }
import com.twitter.storehaus.FutureOps
import com.twitter.storehaus.Store
import com.twitter.util.{ Await, Future, Time }

/**
  * @author Ruban Monu
  */

/** Factory for [[com.twitter.storehaus.mysql.MySqlStore]] instances. */
object MySqlStore {

  def apply(client: Client, table: String, kCol: String, vCol: String) =
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
  * Storehaus-mysql also works with pre-populated MySQL tables, based on the assumption that the key column picked is unique.
  * Any table columns other than the picked key and value columns are ignored during reads and writes.
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
class MySqlStore(client: Client, table: String, kCol: String, vCol: String)
    extends Store[MySqlValue, MySqlValue] {

  protected val SELECT_SQL = "SELECT " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + "=?"
  protected val MULTI_SELECT_SQL_PREFIX = "SELECT " + g(kCol) + ", " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + " IN "

  protected val INSERT_SQL = "INSERT INTO " + g(table) + "(" + g(kCol) + "," + g(vCol) + ")" + " VALUES (?,?)"
  protected val MULTI_INSERT_SQL_PREFIX = "INSERT INTO " + g(table) + "(" + g(kCol) + "," + g(vCol) + ") VALUES "

  protected val UPDATE_SQL = "UPDATE " + g(table) + " SET " + g(vCol) + "=? WHERE " + g(kCol) + "=?"

  // update multiple rows together. e.g.
  // UDPATE table SET value = CASE key
  //   WHEN "key1" THEN "value1"
  //   WHEN "key2" THEN "value2"
  // END
  // WHERE key IN ("key1", "key2")
  protected val MULTI_UPDATE_SQL_PREFIX = "UPDATE " + g(table) + " SET " + g(vCol) + " = CASE " + g(kCol) + " "
  protected val MULTI_UPDATE_SQL_INFIX = " END WHERE " + g(kCol) + " IN "

  protected val DELETE_SQL = "DELETE FROM " + g(table) + " WHERE " + g(kCol) + "=?"
  protected val MULTI_DELETE_SQL_PREFIX = "DELETE FROM " + g(table) + " WHERE " + g(kCol) + " IN "

  protected val START_TXN_SQL = "START TRANSACTION"
  protected val COMMIT_TXN_SQL = "COMMIT"
  protected val ROLLBACK_TXN_SQL = "ROLLBACK"

  // prepared statements to be reused across gets and puts
  // TODO: should this be non-blocking? this is part of object construction, so maybe not?
  protected val selectStmt = Await.result(client.prepare(SELECT_SQL))
  protected val insertStmt = Await.result(client.prepare(INSERT_SQL))
  protected val updateStmt = Await.result(client.prepare(UPDATE_SQL))
  protected val deleteStmt = Await.result(client.prepare(DELETE_SQL))

  protected def startTransaction : Future[Unit] = client.query(START_TXN_SQL).unit
  protected def commitTransaction : Future[Unit] = client.query(COMMIT_TXN_SQL).unit
  protected def rollbackTransaction : Future[Unit] = client.query(ROLLBACK_TXN_SQL).unit


  override def get(k: MySqlValue): Future[Option[MySqlValue]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format
    // we assume here the mysql client already has the dbname/schema selected
    selectStmt.parameters = Array(MySqlStringInjection(k).getBytes)
    val mysqlResult: Future[Seq[Option[MySqlValue]]] = client.select(selectStmt) { row =>
      row(vCol) match { case None => None; case Some(v) => Some(MySqlValue(v)) }
    }
    mysqlResult.map { case result =>
      result.lift(0).flatten.headOption
    }
  }

  override def multiGet[K1 <: MySqlValue](ks: Set[K1]): Map[K1, Future[Option[MySqlValue]]] = {
    if (ks.isEmpty) return Map()
    // build preparedstatement based on keyset size
    val placeholders = Stream.continually("?").take(ks.size).mkString("(", ",", ")")
    val selectSql = MULTI_SELECT_SQL_PREFIX + placeholders
    val mysqlResult: Future[(PreparedStatement,Seq[(Option[MySqlValue], Option[MySqlValue])])] =
        client.prepareAndSelect(selectSql, ks.map(key => MySqlStringInjection(key).getBytes).toSeq:_* ) { row =>
      (row(kCol).map(MySqlValue(_)), row(vCol).map(MySqlValue(_)))
    }
    FutureOps.liftValues(ks,
      mysqlResult.map { case (ps, rows) =>
        client.closeStatement(ps)
        rows.toMap.filterKeys { _ != None }.map { case (optK, optV) => (optK.get, optV) }
      },
      { (k: K1) => Future.None }
    )
  }

  protected def set(k: MySqlValue, v: MySqlValue) = doSet(k, v)

  override def put(kv: (MySqlValue, Option[MySqlValue])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => doSet(key, value).unit
      case (key, None) => doDelete(key).unit
    }
  }

  override def multiPut[K1 <: MySqlValue](kvs: Map[K1, Option[MySqlValue]]): Map[K1, Future[Unit]] = {
    // batched version of put. the batch is split into insert, update, and delete statements.
    // reduce your batch size if you are hitting mysql packet limit:
    // http://dev.mysql.com/doc/refman/5.1/en/packet-too-large.html
    val putResult = startTransaction.flatMap { t =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).map { result =>
        val existingKeys = result.filter { !_._2.isEmpty }.keySet
        val newKeys = result.filter { _._2.isEmpty }.keySet

        // handle inserts for new keys
        val insertF = newKeys.isEmpty match {
          case true => Future(())
          case false =>
            // do not include None values in insert query
            val insertKvs = newKeys.map { k => k -> kvs.getOrElse(k, None) }.filter { ! _._2.isEmpty }
            insertKvs.isEmpty match {
              case true => Future(())
              case false =>
                val insertSql = MULTI_INSERT_SQL_PREFIX + Stream.continually("(?, ?)").take(insertKvs.size).mkString(",")
                val insertParams = insertKvs.map { kv =>
                  List(MySqlStringInjection(kv._1).getBytes, MySqlStringInjection(kv._2.get).getBytes)
                }.toSeq.flatten
                client.prepareAndExecute(insertSql, insertParams:_*).map { case (ps, r) =>
                  // close prepared statement on server
                  client.closeStatement(ps)
                }
            }
        }

        // handle update and/or delete for existing keys
        val existingKvs = existingKeys.map { k => k -> kvs.getOrElse(k, None) }

        // do not include None values in update query
        val updateKvs = existingKvs.filter { ! _._2.isEmpty }
        val updateF = updateKvs.isEmpty match {
          case true => Future(())
          case false =>
            val updateSql = MULTI_UPDATE_SQL_PREFIX + Stream.continually("WHEN ? THEN ?").take(updateKvs.size).mkString(" ") +
              MULTI_UPDATE_SQL_INFIX + Stream.continually("?").take(updateKvs.size).mkString("(", ",", ")")
            val updateParams = updateKvs.map { kv =>
              (MySqlStringInjection(kv._1).getBytes, MySqlStringInjection(kv._2.get).getBytes)
            }
            // params for "WHEN ? THEN ?"
            val updateCaseParams = updateParams.map { kv => List(kv._1, kv._2) }.toSeq.flatten
            // params for "IN (?, ?, ?)"
            val updateInParams = updateParams.map { kv => kv._1 }.toSeq
            client.prepareAndExecute(updateSql, (updateCaseParams ++ updateInParams):_*).map { case (ps, r) =>
              // close prepared statement on server
              client.closeStatement(ps)
            }
        }

        // deletes
        val deleteKeys = existingKvs.filter { _._2.isEmpty }.map { _._1 }
        val deleteF = deleteKeys.isEmpty match {
          case true => Future(())
          case false =>
            val deleteSql = MULTI_DELETE_SQL_PREFIX + Stream.continually("?").take(deleteKeys.size).mkString("(", ",", ")")
            val deleteParams = deleteKeys.map { k => MySqlStringInjection(k).getBytes }.toSeq
            client.prepareAndExecute(deleteSql, deleteParams:_*).map { case (ps, r) =>
              // close prepared statement on server
              client.closeStatement(ps)
            }
        }

        Future.join(List(insertF, updateF, deleteF))
          .flatMap { f => commitTransaction }
          .handle { case e: Exception => rollbackTransaction.flatMap { throw e } }
      }
    }
    kvs.mapValues { v => putResult.flatMap { f => f.unit } }
  }

  override def close(t: Time) = {
    // close prepared statements before closing the connection
    client.closeStatement(selectStmt)
    client.closeStatement(insertStmt)
    client.closeStatement(updateStmt)
    client.closeStatement(deleteStmt)
    client.close(t)
  }

  protected def doSet(k: MySqlValue, v: MySqlValue): Future[Result] = {
    // mysql's insert-or-update syntax works only when a primary key is defined:
    // http://dev.mysql.com/doc/refman/5.1/en/insert-on-duplicate.html
    // since we are not guaranteed that, we first check if key exists
    // and insert or update accordingly
    get(k).flatMap { optionV =>
      optionV match {
        case Some(value) =>
          updateStmt.parameters = Array(MySqlStringInjection(v).getBytes, MySqlStringInjection(k).getBytes)
          client.execute(updateStmt)
        case None =>
          insertStmt.parameters = Array(MySqlStringInjection(k).getBytes, MySqlStringInjection(v).getBytes)
          client.execute(insertStmt)
      }
    }
  }

  protected def doDelete(k: MySqlValue): Future[Result] = {
    deleteStmt.parameters = Array(MySqlStringInjection(k).getBytes)
    client.execute(deleteStmt)
  }

  // enclose table or column names in backticks, in case they happen to be sql keywords
  protected def g(s: String)  = "`" + s + "`"
}
