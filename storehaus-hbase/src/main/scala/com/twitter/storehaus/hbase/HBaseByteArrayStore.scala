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
import com.twitter.util.Future

/**
 * @author MansurAshraf
 * @since 9/8/13
 */
object HBaseByteArrayStore {
  def apply(quorumNames: String, table: String, columnFamily: String, column: String, createTable: Boolean): HBaseByteArrayStore = {
    val store = new HBaseByteArrayStore(quorumNames, table, columnFamily, column, createTable, new HTablePool())
    store.validateConfiguration()
    store.createTableIfRequired()
    store
  }
}

class HBaseByteArrayStore(val quorumNames: String,
                          val table: String,
                          val columnFamily: String,
                          val column: String,
                          val createTable: Boolean,
                          val pool: HTablePool) extends Store[Array[Byte], Array[Byte]] with HBaseStore {

  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: Array[Byte]): Future[Option[Array[Byte]]] = {
    getValue(k)
  }

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (Array[Byte], Option[Array[Byte]])): Future[Unit] = {
    putValue(kv)
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close {
    pool.close()
  }
}
