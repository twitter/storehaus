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

import com.twitter.finagle.exp.mysql.{ Client, PreparedStatement, Result, StringValue }
import com.twitter.storehaus.Store
import com.twitter.util.Future

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8 

/**
 * @author Ruban Monu
 */

object MySQLStore {

  def apply(client: Client, table: String, kCol: String, vCol: String) = 
    new MySQLStore(client, table, kCol, vCol)
}

/**
 * Simple MySQL wrapper.
 * Assumes underlying key and value columns are string types.
 * Value column is treated as a channelbuffer of UTF-8 string.
 */
class MySQLStore(client: Client, table: String, kCol: String, vCol: String)
    extends Store[String, ChannelBuffer] {

  val SELECT_SQL = "SELECT " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + "=?"
  val MULTI_SELECT_SQL_PREFIX = "SELECT " + g(kCol) + ", " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + " IN "
  val INSERT_SQL = "INSERT INTO " + g(table) + "(" + g(kCol) + "," + g(vCol) + ")" + " VALUES (?,?)"
  val UPDATE_SQL = "UPDATE " + g(table) + " SET " + g(vCol) + "=? WHERE " + g(kCol) + "=?"
  val DELETE_SQL = "DELETE FROM " + g(table) + " WHERE " + g(kCol) + "=?"

  override def get(k: String): Future[Option[ChannelBuffer]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format (ChannelBuffer in this case)
    // we assume here the mysql client already has the dbname/schema selected
    val mysqlResult: Future[(PreparedStatement,Seq[ChannelBuffer])] = client.prepareAndSelect(SELECT_SQL, k) { row =>
      val StringValue(v) = row(vCol).get
      // for finagle Value mappings, see:
      // https://github.com/twitter/finagle/blob/master/finagle-mysql/src/main/scala/com/twitter/finagle/mysql/Value.scala
      ChannelBuffers.copiedBuffer(v, UTF_8)
    }
    Future.value(mysqlResult.get._2.lift(0))
  }

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    // build preparedstatement based on keyset size
    val placeholders = new StringBuilder
    for (i <- 1 to ks.size) {
      placeholders.append('?')
      if (i < ks.size) placeholders.append(',')
    }
    val selectSql = MULTI_SELECT_SQL_PREFIX + "(" + placeholders + ")" 
    val mysqlResult: Future[(PreparedStatement,Seq[(String, ChannelBuffer)])] = client.prepareAndSelect(selectSql, ks.toSeq:_*) { row =>
      val StringValue(k) = row(kCol).get
      val StringValue(v) = row(vCol).get
      (k, ChannelBuffers.copiedBuffer(v, UTF_8))
      // we return data in the form of channelbuffers of UTF-8 strings
    }
    val storehausResult = collection.mutable.Map.empty[K1, Future[Option[ChannelBuffer]]]
    for ( row <- mysqlResult.get._2 ) {
      storehausResult += row._1.asInstanceOf[K1] -> Future.value(Some(row._2))
    }
    for ( key <- ks ) {
      if (!storehausResult.contains(key)) {
        storehausResult += key -> Future[Option[ChannelBuffer]](Option.empty)
      }
    }
    storehausResult.toMap
  }

  protected def set(k: String, v: ChannelBuffer) = doSet(k, v).get

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => doSet(key, value).unit
      case (key, None) => doDelete(key).unit
    }
  }
  
  override def close { client.close }

  protected def doSet(k: String, v: ChannelBuffer): Future[Result] = {
    // mysql's insert-or-update syntax works only when a primary key is defined:
    // http://dev.mysql.com/doc/refman/5.1/en/insert-on-duplicate.html
    // since we are not guaranteed that, we first check if key exists
    // and insert or update accordingly
    val getResult = get(k).get
    val (setSql, arg1, arg2) = if (getResult.isEmpty) { (INSERT_SQL, k, v.toString(UTF_8)) }
        else { (UPDATE_SQL, v.toString(UTF_8), k) }
    // prepareAndExecute returns Future[(PreparedStatement,Result)]
    Future.value(client.prepareAndExecute(setSql, arg1, arg2).get._2)
  }

  protected def doDelete(k: String): Future[Result] = {
    // prepareAndExecute returns Future[(PreparedStatement,Result)]
    Future.value(client.prepareAndExecute(DELETE_SQL, k).get._2)
  }

  protected def g(s: String)  = "`" + s + "`"
}
