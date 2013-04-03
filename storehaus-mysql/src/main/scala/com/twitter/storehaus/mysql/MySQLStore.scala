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

import com.twitter.finagle.exp.mysql.Client
import com.twitter.storehaus.Store
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

object MySQLStore {

  def apply(client: Client) = new MySQLStore(client)
}

class MySQLStore(client: Client) extends Store[String, ChannelBuffer] {

  override def get(k: String): Future[Option[ChannelBuffer]] = {
    Future[Option[ChannelBuffer]](Option.empty)
    // TODO: generate and execute sql select query for the input key
    // finagle-mysql select() method lets you pass in a mapping function
    // to convert resultset into desired output format (ChannelBuffer in this case)
    // we assume the mysql client already has the dbname/schema selected
  }

  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[ChannelBuffer]]] = {
    Map[K1, Future[Option[ChannelBuffer]]]()
    // TODO: generate and execute sql select query for the input keys
  }

  protected def set(k: String, v: ChannelBuffer) = {
    // TODO: generate and execute dml query for the input key-value pair
  }

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] = {
    Future[Unit]()
    // TODO: async version of set
  }

  override def close { client.close }
}
