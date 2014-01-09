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
package com.twitter.storehaus.mongodb

import java.util.concurrent.Executors

import com.twitter.storehaus.Store
import com.twitter.util.{ Future, FuturePool }

import com.mongodb.casbah.Imports._

import scala.reflect.Manifest

/**
 *  @author Bin Lan
 */

object MongoStore {

  def apply[K, V: Manifest](
      client: MongoClient,
      dbName: String,
      colName: String,
      keyName: String = "key",
      valueName: String = "value",
      backgroundIndex: Boolean = false,
      threadNumber: Int = 3): MongoStore[K, V] = {
    new MongoStore[K, V](client, dbName, colName, keyName, valueName, backgroundIndex, threadNumber)
  }
}

/**
 * Use this for simple types, e.g. Int, String, Long, use MongoObjectStore for complex objects.
 */
class MongoStore[K, V: Manifest] (
    val client: MongoClient,
    val dbName: String,
    val colName: String,
    val keyName: String,
    val valueName: String,
    val backgroundIndex: Boolean,
    val threadNumber: Int)
  extends Store[K, V] {

  protected val db = client(dbName)
  protected val col = db(colName)
  // make sure we build an index
  col.ensureIndex(MongoDBObject(keyName -> 1), MongoDBObject("name" -> (keyName + "Idx"), "background" -> backgroundIndex))
  protected val futurePool = FuturePool(Executors.newFixedThreadPool(threadNumber))

  override def put(kv: (K, Option[V])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => futurePool {
        col.update(MongoDBObject(keyName -> key), MongoDBObject(keyName -> key, valueName -> value), upsert = true)
      }
      case (key, None) => futurePool {
        col.remove(MongoDBObject(keyName -> key))
      }
    }
  }

  override def get(key: K): Future[Option[V]] = futurePool {
    col.findOne(MongoDBObject(keyName -> key)).flatMap(_.getAs[V](valueName))
  }
}

