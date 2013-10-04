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

package com.twitter.storehaus.asynchbase

import com.twitter.storehaus.Store
import com.twitter.util.Future
import org.hbase.async.HBaseClient

/**
 * @author Mansur Ashraf
 * @since 9/8/13
 */
object AsyncHBaseByteArrayStore {
  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            threads: Int = 4): AsyncHBaseByteArrayStore = {
    val store = new AsyncHBaseByteArrayStore(quorumNames, table, columnFamily, column, new HBaseClient(quorumNames.mkString(",")), threads)
    store.validateConfiguration()
    store
  }

}

class AsyncHBaseByteArrayStore(protected val quorumNames: Seq[String],
                               protected val table: String,
                               protected val columnFamily: String,
                               protected val column: String,
                               protected val client: HBaseClient,
                               protected val threads: Int) extends Store[Array[Byte], Array[Byte]] with AsyncHBaseStore {

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
    client.shutdown().join()
  }
}
