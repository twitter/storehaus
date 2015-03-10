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

import com.twitter.storehaus.Store
import com.twitter.util.{Time, Future, Duration}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

/**
 * Store dealing with a LevelDB database.
 * @author Ben Fradet
 * @since 10/03/15
 */
class LevelDBStore extends Store[String, String] {
  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[String])): Future[Unit] = super.put(kv)

  /** Replace a set of keys at one time */
  override def multiPut[K1 <: String](kvs: Map[K1, Option[String]])
    : Map[K1, Future[Unit]] = super.multiPut(kvs)

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(time: Time): Future[Unit] = super.close(time)

  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[String]] = super.get(k)

  /** Get a set of keys from the store.
    * Important: all keys in the input set are in the resulting map. If the
    * store fails to return a value for a given key, that should be represented
    * by a Future.exception.
    */
  override def multiGet[K1 <: String](ks: Set[K1])
    : Map[K1, Future[Option[String]]] = super.multiGet(ks)

  override def close(after: Duration): Future[Unit] = super.close(after)
}
