/*
 * Copyright 2014 Twitter inc.
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import com.twitter.bijection.hbase.HBaseBijections._
import com.twitter.bijection.Conversion._
import com.twitter.bijection.{Codec, Injection}
import com.twitter.util.{FuturePool, Future}
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import com.twitter.storehaus.FutureOps
import scala.Some
import scala.Seq
import scala.collection.breakOut
import java.util

/**
 * @author Mansur Ashraf
 * @since 9/8/13
 */
trait HBaseStore {

  protected val quorumNames: Seq[String]
  protected val createTable: Boolean
  protected val table: String
  protected val columnFamily: String
  protected val column: String
  protected val pool: HTablePool
  protected val conf: Configuration
  protected val threads: Int
  protected val futurePool = FuturePool(Executors.newFixedThreadPool(threads))

  def getHBaseAdmin: HBaseAdmin = {
    if (conf.get("hbase.zookeeper.quorum") == null) {
      conf.set("hbase.zookeeper.quorum", quorumNames.mkString(","))
    }
    val hbaseConf = HBaseConfiguration.create(conf)
    new HBaseAdmin(hbaseConf)
  }

  def createTableIfRequired() {
    val hbaseAdmin = getHBaseAdmin
    if (createTable && !hbaseAdmin.tableExists(table)) {
      val tableDescriptor = new HTableDescriptor(table)
      tableDescriptor.addFamily(new HColumnDescriptor(columnFamily))
      hbaseAdmin.createTable(tableDescriptor)
    }
  }

  def validateConfiguration() {
    import org.apache.commons.lang.StringUtils.isNotEmpty

    require(!quorumNames.isEmpty, "Zookeeper quorums are required")
    require(isNotEmpty(columnFamily), "column family is required")
    require(isNotEmpty(column), "column is required")
  }


  def getValue[K: Codec, V: Codec](key: K): Future[Option[V]] = {
    val tbl = pool.getTable(table)
    futurePool {
      implicit val fn = implicitly[Codec[K]].toFunction
      val result = tbl.get(createGet(key))
      getValue[V](result)
    } ensure tbl.close
  }

  def putValue[K: Codec, V: Codec](kv: (K, Option[V])): Future[Unit] = {
    val tbl = pool.getTable(table)
    implicit val fn = implicitly[Codec[K]].toFunction

    kv match {
      case (k, Some(v)) => futurePool {
        tbl.put(createPut(k, v))
      } ensure tbl.close

      case (k, None) => futurePool {
        tbl.delete(createDelete(k))
      } ensure tbl.close
    }
  }

  def multiGetValues[K <% Array[Byte], V: Codec](ks: Set[K]): Map[K, Future[Option[V]]] = {
    val tbl = pool.getTable(table)
    val result = futurePool {
      val indexedKeys = ks.map(k1 => (k1: Array[Byte]) -> k1).toMap
      val keys = ks.map(k => createGet(k)).toList.asJava
      val results= tbl.get(keys)

      results.toList.map {
        r =>
          val key = indexedKeys(r.getRow)
          val value = getValue[V](r)
          (key, value)
      }.toMap
    } ensure tbl.close
    FutureOps.liftValues(ks, result, k => Future(None))
  }

  def multiPutValues[K <% Array[Byte], V: Codec](kvs: Map[K, Option[V]]): Map[K, Future[Unit]] = {
    val tbl = pool.getTable(table)
    val result = futurePool {
      val (puts, deletes) = kvs.partition(_._2.isDefined)

      if (!puts.isEmpty) {
        tbl.put(puts.map {
          case (k, Some(v)) => createPut(k, v)
        }.toList.asJava)
      }

      if (!deletes.isEmpty) {
        tbl.delete(deletes.map {
          case (k, None) => createDelete[K](k)
        }.toBuffer.asJava)
      }

    } ensure tbl.close
    kvs.map {
      case (k, _) => k -> result
    }(breakOut)
  }

  private def createGet[K<% Array[Byte]](key: K): Get = {
    val g = new Get(key)
    g.addColumn(columnFamily.as[StringBytes], column.as[StringBytes])
    g
  }

  private def getValue[V: Codec](result: Result): Option[V] = {
    val value = result.getValue(columnFamily.as[StringBytes], column.as[StringBytes])
    Option(value).map(v => Injection.invert[V, Array[Byte]](v).get)
  }

  private def createPut[K <% Array[Byte], V: Codec](key: K, value: V): Put = {
    val p = new Put(key)
    p.add(columnFamily.as[StringBytes], column.as[StringBytes], Injection[V, Array[Byte]](value))
    p
  }

  private def createDelete[K <% Array[Byte]](key: K): Delete = {
    val delete = new Delete(key)
    delete.deleteColumn(columnFamily.as[StringBytes], column.as[StringBytes])
    delete
  }
}
