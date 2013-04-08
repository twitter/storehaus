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

import java.net.{ ServerSocket, BindException }
import java.util.logging.{ Level, Logger }
import java.util.Properties

import com.twitter.finagle.exp.mysql.{ Client, OK }
import com.twitter.util.Future
import com.twitter.util.NonFatal

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * based on finagle-mysql ClientTest
 */
object ConnectionSettings {
  private val logger = Logger.getLogger("storehaus-mysql-test")

  val p = new Properties
  try {
    val resource = getClass.getResource("/integration.properties")
    if (resource == null)
      logger.log(Level.WARNING, "integration.properties not found")
    else
      p.load(resource.openStream())
  } catch {
    case NonFatal(e) =>
      logger.log(Level.WARNING, "Exception while loading integration.properties", e)
  }

  // Requires mysql running @ localhost:3306
  // It's likely that mysqld is running if 3306 bind fails.
  // TODO: unit test without actually requiring a local mysql instance?
  val isAvailable = try {
    val socket = new ServerSocket(3306)
    socket.close()
    false
  } catch {
    case e: BindException => true
  }
}

object StorehausMySQL {
  private val logger = Logger.getLogger("storehaus-mysql-test")

  val mysqlClient: Option[Client] = if (ConnectionSettings.isAvailable) {
    logger.log(Level.INFO, "Attempting to connect to mysqld @ localhost:3306")
    val username = ConnectionSettings.p.getProperty("username", "<user>")
    val password = ConnectionSettings.p.getProperty("password", "<password>")
    val db = ConnectionSettings.p.getProperty("db", "test")
    Some(Client("localhost:3306", username, password, db))
  } else {
    None
  }

  val mysqlStore: Option[MySQLStore] = if (mysqlClient.isDefined) {
    Some(MySQLStore(mysqlClient.get, "storehaus-mysql-test", "key", "value")) 
  } else {
    None
  }

  def close() = { mysqlStore.get.close } 
}

class MySQLStoreTest extends FunSuite with BeforeAndAfterAll {

  val schema = """CREATE TEMPORARY TABLE IF NOT EXISTS `storehaus-mysql-test` (
        `key` varchar(40) DEFAULT NULL,
        `value` varchar(100) DEFAULT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

  test("Create table") {
    val createResult = StorehausMySQL.mysqlClient.get.query(schema).get
    expectResult(true) { createResult.isInstanceOf[OK] }
  }

  test("Single put, get and delete") {
    StorehausMySQL.mysqlStore.get.put(("key1", Some(ChannelBuffers.copiedBuffer("value1", UTF_8)))).get
    var getResult = StorehausMySQL.mysqlStore.get.get("key1").get
    expectResult("value1") { getResult.get.toString(UTF_8) }
    StorehausMySQL.mysqlStore.get.put(("key1", None)).get
    getResult = StorehausMySQL.mysqlStore.get.get("key1").get
    expectResult(None) { getResult }
  }

  test("Multi get") {
    StorehausMySQL.mysqlStore.get.put(("key1", Some(ChannelBuffers.copiedBuffer("value1", UTF_8)))).get
    StorehausMySQL.mysqlStore.get.put(("key2", Some(ChannelBuffers.copiedBuffer("value2", UTF_8)))).get
    val multiGetResult = StorehausMySQL.mysqlStore.get.multiGet( Set("key1", "key2", "key3") )
    expectResult(ChannelBuffers.copiedBuffer("value1", UTF_8)) { multiGetResult.get("key1").get.get.get }
    expectResult(ChannelBuffers.copiedBuffer("value2", UTF_8)) { multiGetResult.get("key2").get.get.get }
    expectResult(None) { multiGetResult.get("key3").get.get }
  }
  
  override def afterAll() { StorehausMySQL.close() }
}
