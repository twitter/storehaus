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
import com.twitter.util.{Future, Time}
import com.twitter.bijection.Injection._
import org.hbase.async.HBaseClient

/**
 * @author Mansur Ashraf
 * @since 9/29/13
 */
object AsyncHBaseStringStore {
  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String,
            threads: Int=4): AsyncHBaseStringStore = {
    val store = new AsyncHBaseStringStore(quorumNames, table, columnFamily, column, new HBaseClient(quorumNames.mkString(",")), threads)
    store.validateConfiguration()
    store
  }
}

class AsyncHBaseStringStore(protected val quorumNames: Seq[String],
                            protected val table: String,
                            protected val columnFamily: String,
                            protected val column: String,
                            protected val client: HBaseClient,
                            protected val threads: Int) extends Store[String, String] with AsyncHBaseStore {
  import com.twitter.bijection.hbase.HBaseInjections.string2BytesInj
  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[String]] =
    getValue(k)(string2BytesInj, string2BytesInj)

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[String])): Future[Unit] =
    putValue(kv)(string2BytesInj, string2BytesInj)

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(t: Time) = futurePool {
    client.shutdown()
  }
}

