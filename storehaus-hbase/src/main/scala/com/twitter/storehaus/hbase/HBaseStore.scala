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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import com.twitter.bijection.hbase.HBaseBijections._
import com.twitter.bijection.Conversion._
import com.twitter.bijection.Injection
import com.twitter.util.Future
import scala.Some

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

  def getValue[K, V](key: K)(implicit keyInj: Injection[K, Array[Byte]], valueInj: Injection[V, Array[Byte]]): Future[Option[V]] = Future {
    val tbl = pool.getTable(table)
    val g = new Get(keyInj(key))
    g.addColumn(columnFamily.as[StringBytes], column.as[StringBytes])

    val result = tbl.get(g)
    val value = result.getValue(columnFamily.as[StringBytes], column.as[StringBytes])
    Option(value).map(v => valueInj.invert(v).get)
  }

  def putValue[K, V](kv: (K, Option[V]))(implicit keyInj: Injection[K, Array[Byte]], valueInj: Injection[V, Array[Byte]]): Future[Unit] = {
    kv match {
      case (k, Some(v)) => Future {
        val p = new Put(keyInj(k))
        p.add(columnFamily.as[StringBytes], column.as[StringBytes], valueInj(v))
        val tbl = pool.getTable(table)
        tbl.put(p)
      }
      case (k, None) => Future {
        val delete = new Delete(keyInj(k))
        val tbl = pool.getTable(table)
        tbl.delete(delete)
      }
    }
  }
}
