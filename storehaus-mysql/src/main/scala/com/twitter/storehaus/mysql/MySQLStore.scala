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
import com.twitter.util.Future

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

  val SELECT_SQL = "SELECT " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + "=?"
  val MULTI_SELECT_SQL_PREFIX = "SELECT " + g(kCol) + ", " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + " IN "
  val INSERT_SQL = "INSERT INTO " + g(table) + "(" + g(kCol) + "," + g(vCol) + ")" + " VALUES (?,?)"
  val UPDATE_SQL = "UPDATE " + g(table) + " SET " + g(vCol) + "=? WHERE " + g(kCol) + "=?"
  val DELETE_SQL = "DELETE FROM " + g(table) + " WHERE " + g(kCol) + "=?"

  override def get(k: MySqlValue): Future[Option[MySqlValue]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format
    // we assume here the mysql client already has the dbname/schema selected
    val mysqlResult: Future[(PreparedStatement,Seq[Option[MySqlValue]])] = client.prepareAndSelect(SELECT_SQL, MySqlStringInjection(k).getBytes) { row =>
      row(vCol) match { case None => None; case Some(v) => Some(MySqlValue(v)) }
    }
    mysqlResult.map { case(ps, result) =>
      client.closeStatement(ps)
      result.lift(0).getOrElse(Option.empty)
    }
  }

  override def multiGet[K1 <: MySqlValue](ks: Set[K1]): Map[K1, Future[Option[MySqlValue]]] = {
    if (ks.isEmpty) return Map()
    // build preparedstatement based on keyset size
    val placeholders = Stream.continually("?").take(ks.size).mkString("(", ",", ")")
    val selectSql = MULTI_SELECT_SQL_PREFIX + placeholders
    val mysqlResult: Future[(PreparedStatement,Seq[(Option[MySqlValue], Option[MySqlValue])])] =
        client.prepareAndSelect(selectSql, ks.map(key => MySqlStringInjection(key).getBytes).toSeq:_* ) { row =>
      (row(kCol) match { case None => None; case Some(k) => Some(MySqlValue(k)) },
       row(vCol) match { case None => None; case Some(v) => Some(MySqlValue(v)) })
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
  
  override def close { client.close }

  protected def doSet(k: MySqlValue, v: MySqlValue): Future[Result] = {
    // mysql's insert-or-update syntax works only when a primary key is defined:
    // http://dev.mysql.com/doc/refman/5.1/en/insert-on-duplicate.html
    // since we are not guaranteed that, we first check if key exists
    // and insert or update accordingly
    get(k).flatMap { optionV =>
      optionV match {
        case Some(value) => client.prepareAndExecute(UPDATE_SQL, MySqlStringInjection(v).getBytes, MySqlStringInjection(k).getBytes)
        case None => client.prepareAndExecute(INSERT_SQL, MySqlStringInjection(k).getBytes, MySqlStringInjection(v).getBytes)
      }
      // prepareAndExecute returns Future[(PreparedStatement,Result)]
    }.map { case (ps, result) => client.closeStatement(ps); result }
  }

  protected def doDelete(k: MySqlValue): Future[Result] = {
    // prepareAndExecute returns Future[(PreparedStatement,Result)]
    client.prepareAndExecute(DELETE_SQL, MySqlStringInjection(k).getBytes).map {
      case (ps, result) => client.closeStatement(ps); result
    }
  }

  // enclose table or column names in backticks, in case they happen to be sql keywords
  protected def g(s: String)  = "`" + s + "`"
}
