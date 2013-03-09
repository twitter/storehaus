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

package com.twitter.storehaus.berkeleydb

import com.sleepycat.je.{ Database, DatabaseEntry, LockMode, OperationStatus }
import com.twitter.storehaus.{ ReadableStore, Store }
import com.twitter.util.Future

/**
  * Storehaus wrapper over a BerkeleyDB key-value store.
  *
  *  @author Sam Ritchie
  */

object BDBStore {
  def readable(db: Database) = new BDBReadableStore(db)
  def apply(db: Database) = new DBDStore(db)
}

class BDBReadableStore private (db: Database) extends ReadableStore[Array[Byte], Array[Byte]] {
  import OperationStatus.{ SUCCESS, NOTFOUND }

  assert(db.getConfig.getReadOnly, "Database must be configured in read-only mode.")

  override def get(k: String): Future[Option[Array[Byte]]] = {
    val valueHolder = new DatabaseEntry
    try {
      db.get(null, new DatabaseEntry(k), valueHolder, LockMode.READ_UNCOMMITTED).match {
        case SUCCESS => Future.value(Some(valueHolder.getData))
        case _ => Future.None
      } catch {
        case e: Throwable => Future.exception(e)
      }
    }
  }

  override def close {
    db.getEnvironment.close
    db.close
  }
}

class BDBStore(db: Database) extends BDBReadableStore(db) {
  assert(!db.getConfig.getReadOnly, "Database must be configured in read-write mode.")

  override def put(kv: (String, Option[Array[Byte]])): Future[Unit] =
    try {
      val keyEntry = new DatabaseEntry(key)
      kv match {
        case (key, Some(value)) => db.put(null, keyEntry, new DatabaseEntry(value))
        case (key, None) => db.delete(null, keyEntry)
      }
      Future.Unit
    } catch {
      case e: Throwable => Future.exception(e)
    }

  override def close {
    val env = db.getEnvironment
    env.sync
    var anyCleaned = false;
    while (env.cleanLog > 0) { anyCleaned = true }
    if (anyCleaned) {
      val checkpoint = new CheckpointConfig
      checkpoint.setForce(true)
      env.checkpoint(checkpoint)
    }
    super.close
  }
}
