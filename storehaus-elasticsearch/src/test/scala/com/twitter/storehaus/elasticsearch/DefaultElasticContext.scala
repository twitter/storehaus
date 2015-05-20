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

package com.twitter.storehaus.elasticsearch

import org.elasticsearch.common.settings.ImmutableSettings
import java.util.UUID
import java.io.File
import org.elasticsearch.node.NodeBuilder._
import org.json4s.{native, NoTypeHints}

/**
 * @author Mansur Ashraf
 * @since 1/13/14
 */
trait DefaultElasticContext {

  val tempFile = File.createTempFile("elasticsearchtests", "tmp")
  val homeDir = new File(tempFile.getParent + "/" + UUID.randomUUID().toString)
  val test_index = "test_index"
  val test_type = "test_type"
  val DEFAULT_TIMEOUT = 10 * 1000

  homeDir.mkdir()
  homeDir.deleteOnExit()
  tempFile.deleteOnExit()

  val settings = ImmutableSettings.settingsBuilder()
    .put("node.http.enabled", false)
    .put("http.enabled", false)
    .put("path.home", homeDir.getAbsolutePath)
    .put("index.number_of_shards", 1)
    .put("index.number_of_replicas", 0)

  val client = {
    val node = nodeBuilder().local(true).data(true).settings(settings).node()
    node.client().admin().cluster().prepareHealth()
      .setWaitForYellowStatus().execute().actionGet()
    node.client()
  }
  private implicit val formats = native.Serialization.formats(NoTypeHints)
  lazy val store = ElasticSearchCaseClassStore[Person](test_index, test_type, client)

  def refreshIndex(): Unit = {
    refresh(test_index)
  }

  def refresh(indexes: String*): Unit = {
    val i = indexes.size match {
      case 0 => Seq("_all")
      case _ => indexes
    }
    val listener = client.admin().indices().prepareRefresh(i: _*).execute()
    listener.actionGet()
  }

  def block(duration: Long) = Thread.sleep(duration)

  def blockAndRefreshIndex = {
    block(DEFAULT_TIMEOUT)
    refreshIndex()
  }
}

case class Person(fname: String, lname: String, age: Int)

case class Book(name: String, authors: Array[String], published: Int)
