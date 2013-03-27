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

import java.util.{ Map => JMap }
import scala.collection.JavaConverters._

/**
  * Basic cache implementation
  */

class JMapCache[K, V](val m: JMap[K, V]) extends Cache[K, V] {
  def get(k: K) = Option(m.get(k))
  override def contains(k: K) = m.containsKey(k)
  def hit(k: K) = this
  def miss(kv: (K, V)) = { m.put(kv._1, kv._2); this }
  def evict(k: K) = { m.remove(k); this }
  def seed[T <: K](seed: Map[T, V]) = {
    m.clear
    m.putAll(seed.asJava)
    this
  }
  override def toString = m.toString
}
