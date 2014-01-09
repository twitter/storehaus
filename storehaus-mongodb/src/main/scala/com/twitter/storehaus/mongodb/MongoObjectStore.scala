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

import scala.util.Try

import com.twitter.bijection.{ Injection, AbstractInjection }
import com.twitter.storehaus.{ Store, ConvertedStore }
import com.twitter.util.Future

import com.mongodb.casbah.Imports._

import scala.reflect.Manifest

/**
 *  @author Bin Lan
 */

trait MongoInjection[T] extends AbstractInjection[T, MongoDBObject]

object MongoObjectStore {

  val internalInjection: Injection[MongoDBObject, DBObject] = new AbstractInjection[MongoDBObject, DBObject] {
    override def apply(in: MongoDBObject): DBObject = in.asDBObject
    override def invert(in: DBObject): Try[MongoDBObject] = Try(new MongoDBObject(in))
  }

  def apply[K, V](
      client: MongoClient,
      dbName: String,
      colName: String,
      keyName: String = "key",
      valueName: String = "value",
      backgroundIndex: Boolean = false,
      threadNumber: Int = 3)(implicit inj: MongoInjection[V]): MongoObjectStore[K, V] = {
    val injection = Injection.connect[V, MongoDBObject, DBObject](inj, internalInjection)
    val store = new MongoStore[K, DBObject](client, dbName, colName, keyName, valueName, backgroundIndex, threadNumber)
    new MongoObjectStore[K, V](store)(injection)
  }
}

class MongoObjectStore[K, V](val underlying: MongoStore[K, DBObject])(implicit injection: Injection[V, DBObject])
  extends ConvertedStore[K, K, DBObject, V](underlying)(identity)(injection)

