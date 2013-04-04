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

import com.twitter.finagle.exp.mysql.{ Client, Result, StringValue }
import com.twitter.storehaus.Store
import com.twitter.util.Future

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8 

object MySQLStore {

  def apply(client: Client, table: String, kCol: String, vCol: String) = 
    new MySQLStore(client, table, kCol, vCol)
}

class MySQLStore(client: Client, table: String, kCol: String, vCol: String)
    extends Store[String, ChannelBuffer] {

  override def get(k: String): Future[Option[ChannelBuffer]] = {
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format (ChannelBuffer in this case)
    // we assume here the mysql client already has the dbname/schema selected
    val selectSql = "SELECT " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + "=" + q(k)
    val mysqlResult: Future[Seq[ChannelBuffer]] = client.select(selectSql) { row =>
      val StringValue(v) = row(vCol).get
      ChannelBuffers.copiedBuffer(v, UTF_8)
    }
    Future.value(mysqlResult.get.lift(0))
  }

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    val keyList = ks.map(q(_)).mkString(",")
    val selectSql = "SELECT " + g(kCol) + ", " + g(vCol) + " FROM " + g(table) + " WHERE " + g(kCol) + " IN (" + keyList + ")"
    val mysqlResult: Future[Seq[(String, ChannelBuffer)]] = client.select(selectSql) { row =>
      val StringValue(k) = row(kCol).get
      val StringValue(v) = row(vCol).get
      (k, ChannelBuffers.copiedBuffer(v, UTF_8))
    }
    val storehausResult = collection.mutable.Map.empty[K1, Future[Option[ChannelBuffer]]]
    for ( row <- mysqlResult.get ) {
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
    val setSql = if (getResult.isEmpty) {
        "INSERT INTO " + g(table) + "(" + g(kCol) + "," + g(vCol) + ")" + " VALUES (" + q(k) + "," + q(v.toString(UTF_8)) +")"
      } else {
        "UPDATE " + g(table) + " SET " + g(vCol) + "=" + q(v.toString(UTF_8)) + " WHERE " + g(kCol) + "=" + q(k)
      }
    client.query(setSql)
  }

  protected def doDelete(k: String): Future[Result] = {
    val deleteSql = "DELETE FROM " + g(table) + " WHERE " + g(kCol) + "=" + q(k)
    client.query(deleteSql)
  }

  protected def q(s: String)  = "'" + s + "'"

  protected def g(s: String)  = "`" + s + "`"
}
