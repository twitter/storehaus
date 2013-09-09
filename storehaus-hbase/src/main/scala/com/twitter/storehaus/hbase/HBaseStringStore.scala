/*
 * Copyright 2013 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.hbase

import org.apache.commons.lang.StringUtils._
import org.apache.hadoop.hbase.client._
import com.twitter.storehaus.Store
import com.twitter.util.Future
import com.twitter.bijection.Conversion._
import com.twitter.bijection.hbase.HBaseBijections._
import com.twitter.bijection.Bijection
import scala.Some

/**
 * @author MansurAshraf
 * @since 9/7/13
 */
object HBaseStringStore {
  def apply(quorumNames: String, table: String, columnFamily: String, column: String, createTable: Boolean): HBaseStringStore = {
    val stringStore = new HBaseStringStore(quorumNames, table, columnFamily, column, createTable, new HTablePool())
    stringStore.createTableIfRequired()
    stringStore
  }
}

class HBaseStringStore(val quorumNames: String, val table: String, val columnFamily: String, val column: String, val createTable: Boolean, val pool: HTablePool) extends Store[String, String] with HBaseStoreConfig {

  require(isNotEmpty(quorumNames), "Zookeeper quorums are required")
  require(isNotEmpty(columnFamily), "column family is required")


  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[String]] = Future[Option[String]] {
    val tbl = pool.getTable(table)
    val g = createGetRequest(k)
    val result = tbl.get(g)
    extractValue(result)
  }


  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[String])): Future[Unit] = {
    kv match {
      case (k, Some(v)) => {
        Future {
          val p = new Put(k.as[StringBytes])
          p.add(columnFamily.as[StringBytes], column.as[StringBytes], v.as[StringBytes])
          val tbl = pool.getTable(table)
          tbl.put(p)
        }
      }
      case (k, None) => Future {
        val delete = new Delete(k.as[StringBytes])
        val tbl = pool.getTable(table)
        tbl.delete(delete)
      }
    }

  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close {
    pool.close()
  }

  def createGetRequest(k: String): Get = {
    val g = new Get(k.as[StringBytes])
    g.addColumn(columnFamily.as[StringBytes], column.as[StringBytes])
    g
  }

  def extractValue(result: Result): Option[String] = {
    val value = result.getValue(columnFamily.as[StringBytes], column.as[StringBytes])
    Option(value) map (v => Bijection.invert[String, StringBytes](v.asInstanceOf[StringBytes]))
  }

}
