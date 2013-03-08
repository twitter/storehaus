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

package com.twitter.storehaus

import com.twitter.util.Future
import java.util.{ Map => JMap, HashMap => JHashMap }
/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class JMapStore[K, V] extends Store[K, V] {
  protected val jstore: JMap[K, Option[V]] = new JHashMap[K, Option[V]]()
  protected def storeGet(k: K): Option[V] = {
    val stored = jstore.get(k)
    if (stored != null)
      stored
    else
      None
  }
  override def get(k: K): Future[Option[V]] = Future.value(storeGet(k))
  override def put(kv: (K, Option[V])) = {
    if (kv._2.isEmpty) jstore.remove(kv._1) else jstore.put(kv._1, kv._2)
    Future.Unit
  }
}
