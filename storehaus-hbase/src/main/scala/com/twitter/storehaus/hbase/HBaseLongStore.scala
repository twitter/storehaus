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

import org.apache.hadoop.hbase.client.{Delete, Put, Result, HTablePool}
import com.twitter.storehaus.Store
import org.apache.commons.lang.StringUtils._
import com.twitter.util.Future
import com.twitter.bijection.hbase.HBaseBijections._
import com.twitter.bijection.Bijection
import com.twitter.bijection.Conversion._

/**
 * @author MansurAshraf
 * @since 9/8/13
 */
object HBaseLongStore {
  def apply(quorumNames: String, table: String, columnFamily: String, column: String, createTable: Boolean): HBaseLongStore = {
    val store = new HBaseLongStore(quorumNames, table, columnFamily, column, createTable, new HTablePool())
    store.createTableIfRequired()
    store
  }
}

class HBaseLongStore(val quorumNames: String, val table: String, val columnFamily: String, val column: String, val createTable: Boolean, val pool: HTablePool) extends Store[String, Long] with HBaseStoreConfig {

  require(isNotEmpty(quorumNames), "Zookeeper quorums are required")
  require(isNotEmpty(columnFamily), "column family is required")
  require(isNotEmpty(column), "column  is required")


  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[Long]] = Future[Option[Long]] {
    val tbl = pool.getTable(table)
    val g = createGetRequest(k.as[StringBytes])
    val result = tbl.get(g)
    extractValue(result)
  }


  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[Long])): Future[Unit] = {
    kv match {
      case (k, Some(v)) => {
        Future {
          val p = new Put(k.as[StringBytes])
          p.add(columnFamily.as[StringBytes], column.as[StringBytes], v.as[LongBytes])
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

  def extractValue(result: Result): Option[Long] = {
    val value = result.getValue(columnFamily.as[StringBytes], column.as[StringBytes])
    Option(value) map (v => Bijection.invert[Long, LongBytes](v.asInstanceOf[LongBytes]))
  }

}

