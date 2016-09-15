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

package com.twitter.storehaus.asynchbase

import com.twitter.util._
import com.twitter.bijection.{Injection, Codec, Conversion}
import Conversion._
import org.hbase.async.{DeleteRequest, PutRequest, GetRequest, HBaseClient}

import scala.collection.JavaConverters._

/**
 * @author Mansur Ashraf
 * @since 9/28/13
 */
trait AsyncHBaseStore {

  protected val quorumNames: Seq[String]
  protected val table: String
  protected val columnFamily: String
  protected val column: String
  protected val threads: Int
  protected val client: HBaseClient

  def validateConfiguration() {
    import org.apache.commons.lang.StringUtils.isNotEmpty

    require(quorumNames.nonEmpty, "Zookeeper quorums are required")
    require(isNotEmpty(columnFamily), "column family is required")
    require(isNotEmpty(column), "column is required")
    require(isNotEmpty(table), "table is required")
    client.ensureTableExists(table)
  }

  import DeferredToFutureConverter.toFuture
  def getValue[K: Codec, V: Codec](key: K): Future[Option[V]] = {
    val request = new GetRequest(table.as[Array[Byte]], key.as[Array[Byte]])
      .family(columnFamily.as[Array[Byte]])
      .qualifier(column.as[Array[Byte]])

    val kvFuture = toFuture(client.get(request))
    kvFuture.map { kv =>
      kv.asScala
        .headOption
        .map(kv => Injection.invert[V, Array[Byte]](kv.value()).get)
    }
  }

  def putValue[K: Codec, V: Codec](kv: (K, Option[V])): Future[Unit] = {
    kv match {
      case (k, Some(v)) =>
        val put = new PutRequest(table.as[Array[Byte]],
          k.as[Array[Byte]],
          columnFamily.as[Array[Byte]],
          column.as[Array[Byte]],
          v.as[Array[Byte]])
        toFuture(client.put(put)).unit
      case (k, None) =>
        val delete = new DeleteRequest(table.as[Array[Byte]],
          k.as[Array[Byte]],
          columnFamily.as[Array[Byte]],
          column.as[Array[Byte]])
        toFuture(client.delete(delete)).unit
    }
  }
}
