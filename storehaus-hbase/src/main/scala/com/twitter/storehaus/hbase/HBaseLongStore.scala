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

import org.apache.hadoop.hbase.client.HTablePool
import com.twitter.storehaus.Store
import com.twitter.util.{Future, Time}
import org.apache.hadoop.conf.Configuration

/**
 * @author Mansur Ashraf
 * @since 9/8/13
 */
object HBaseLongStore {
  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            createTable: Boolean,
            pool: HTablePool,
            conf: Configuration,
            threads:Int): HBaseLongStore = {
    val store = new HBaseLongStore(quorumNames, table, columnFamily, column, createTable, pool, conf,threads)
    store.validateConfiguration()
    store.createTableIfRequired()
    store
  }

  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            createTable: Boolean): HBaseLongStore = apply(quorumNames, table, columnFamily, column, createTable, new HTablePool(), new Configuration(),4)
}

class HBaseLongStore(protected val quorumNames: Seq[String],
                     protected val table: String,
                     protected val columnFamily: String,
                     protected val column: String,
                     protected val createTable: Boolean,
                     protected val pool: HTablePool,
                     protected val conf: Configuration,
                     protected val threads:Int) extends Store[String, Long] with HBaseStore {

  import com.twitter.bijection.hbase.HBaseInjections.{string2BytesInj, long2BytesInj}
  /** get a single key from the store.
    */
  override def get(k: String): Future[Option[Long]] =
    getValue[String, Long](k)(string2BytesInj, long2BytesInj)

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[Long])): Future[Unit] =
    putValue(kv)(string2BytesInj, long2BytesInj)

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(t: Time) = futurePool {
    pool.close()
  }
}

