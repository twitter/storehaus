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

import org.apache.hadoop.hbase.client._
import com.twitter.storehaus.Store
import com.twitter.util.Future
import com.twitter.bijection.Injection._
import org.apache.hadoop.conf.Configuration

/**
 * @author MansurAshraf
 * @since 9/7/13
 */
object HBaseStringStore {
  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            createTable: Boolean,
            pool: HTablePool,
            conf: Configuration): HBaseStringStore = {
    val store = new HBaseStringStore(quorumNames, table, columnFamily, column, createTable, pool, conf)
    store.validateConfiguration()
    store.createTableIfRequired()
    store
  }

  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            createTable: Boolean): HBaseStringStore = apply(quorumNames, table, columnFamily, column, createTable, new HTablePool(), new Configuration())
}

class HBaseStringStore(protected val quorumNames: Seq[String],
                       protected val table: String,
                       protected val columnFamily: String,
                       protected val column: String,
                       protected val createTable: Boolean,
                       protected val pool: HTablePool,
                       protected val conf: Configuration) extends Store[String, String] with HBaseStore {
  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[String]] = {
    import com.twitter.bijection.hbase.HBaseBijections._
    implicit val stringInj = fromBijectionRep[String, StringBytes]
    getValue[String, String](k)
  }

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[String])): Future[Unit] = {
    import com.twitter.bijection.hbase.HBaseBijections._
    implicit val stringInj = fromBijectionRep[String, StringBytes]
    putValue(kv)
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close {
    pool.close()
  }
}
