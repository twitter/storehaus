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

import com.twitter.storehaus.Store
import com.twitter.util.Future

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.Imports._

import scala.reflect._

/**
 *  @author Bin Lan
 */

object MongoStore {

  def apply[K, V](client: MongoClient, dbName: String, colName: String, keyName: String = "key", valueName: String = "value"): MongoStore[K, V] = {
    new MongoStore[K, V](client, dbName, colName, keyName, valueName)
  }

}

class MongoStore[K, V] (val client: MongoClient, val dbName: String, val colName: String, val keyName: String, val valueName: String)
  extends Store[K, V] {

  protected val db = client(dbName)
  protected val col = db(colName)
  // make sure we build an index
  col.ensureIndex(MongoDBObject(keyName -> 1), keyName+"Idx", unique = true)

  private def getAs[V: Manifest](key: K): Option[V] = {
    col.findOne(MongoDBObject(keyName -> key)).flatMap(_.getAs[V](valueName))
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => {
        col.update(MongoDBObject(keyName -> key), MongoDBObject(keyName -> key, valueName -> value), upsert = true)
      }
      case (key, None) => {
        col remove MongoDBObject(keyName -> key)
      }
    }
    Future.Unit
  }

  override def get(key: K): Future[Option[V]] = {
    Future.value(getAs(key))
  }

}
