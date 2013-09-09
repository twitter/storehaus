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

import org.apache.hadoop.hbase.client.{Get, HBaseAdmin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import com.twitter.bijection.hbase.HBaseBijections._
import com.twitter.bijection.Conversion._

/**
 * @author MansurAshraf
 * @since 9/8/13
 */
trait HBaseStoreConfig {

  val quorumNames: String
  val createTable: Boolean
  val table: String
  val columnFamily: String
  val column:String

  def getHBaseAdmin: HBaseAdmin = {
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", quorumNames)
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


  def createGetRequest(k: Array[Byte]): Get = {
    val g = new Get(k)
    g.addColumn(columnFamily.as[StringBytes], column.as[StringBytes])
    g
  }

}
