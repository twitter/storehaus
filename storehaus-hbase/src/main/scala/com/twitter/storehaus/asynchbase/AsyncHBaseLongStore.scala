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
 * @since 9/8/13
 */
object AsyncHBaseLongStore {
  def apply(quorumNames: Seq[String],
            table: String,
            columnFamily: String,
            column: String): AsyncHBaseLongStore = {
    val store = new AsyncHBaseLongStore(quorumNames, table, columnFamily, column, new HBaseClient(quorumNames.mkString(",")))
    store.validateConfiguration()
    store
  }


}

class AsyncHBaseLongStore(protected val quorumNames: Seq[String],
                          protected val table: String,
                          protected val columnFamily: String,
                          protected val column: String,
                          protected val client: HBaseClient) extends Store[String, Long] with AsyncHBaseStore {


  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[Long]] = {
    import com.twitter.bijection.hbase.HBaseBijections._
    implicit val stringInj = fromBijectionRep[String, StringBytes]
    implicit val LongInj = fromBijectionRep[Long, LongBytes]
    getValue[String, Long](k)
  }

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[Long])): Future[Unit] = {
    import com.twitter.bijection.hbase.HBaseBijections._
    implicit val stringInj = fromBijectionRep[String, StringBytes]
    implicit val LongInj = fromBijectionRep[Long, LongBytes]
    putValue(kv)
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(t: Time) = client.shutdown().fut.map(_ => ())
}

