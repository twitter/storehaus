/*
 * Copyright 2015 Twitter inc.
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

package com.twitter.storehaus.leveldb

import java.io.File
import java.util.concurrent.Executors

import com.twitter.storehaus.Store
import com.twitter.util.{FuturePool, Time, Future, Duration}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

/**
 * Store dealing with a LevelDB database.
 * @author Ben Fradet
 * @since 10/03/15
 */
class LevelDBStore(val dir: File, val options: Options, val numThreads: Int)
    extends Store[Array[Byte], Array[Byte]] {

  private lazy val db = factory.open(dir, options)
  private val futurePool = FuturePool(Executors.newFixedThreadPool(numThreads))

  /** Get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time.
    */
  override def get(k: Array[Byte]): Future[Option[Array[Byte]]] = futurePool {
    require(k != null)
    db.get(k) match {
      case a: Array[Byte] => Some(a)
      case _ => None
    }
  }

  /** Get a set of keys from the store.
    * Important: all keys in the input set are in the resulting map. If the
    * store fails to return a value for a given key, that should be represented
    * by a Future.exception.
    */
  override def multiGet[K1 <: Array[Byte]](ks: Set[K1])
      : Map[K1, Future[Option[Array[Byte]]]] = super.multiGet(ks)

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (Array[Byte], Option[Array[Byte]])): Future[Unit] = {
    require(kv._1 != null)
    kv match {
      case (k, Some(v)) => futurePool {
        db.put(k, v)
      }
      case (k, None) => futurePool {
        db.delete(k)
      }
    }
  }

  /** Replace a set of keys at one time */
  override def multiPut[K1 <: Array[Byte]](kvs: Map[K1, Option[Array[Byte]]])
      : Map[K1, Future[Unit]] = {
    val future = futurePool {
      val batch = db.createWriteBatch()
      kvs.foreach(kv => kv match {
        case (k, Some(v)) => batch.put(k, v)
        case (k, None) => batch.delete(k)
      })
      db.write(batch)
      batch.close()
    }
    kvs.mapValues(_ => future)
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(time: Time): Future[Unit] = {
    futurePool { db.close() }
    super.close(time)
  }
}
