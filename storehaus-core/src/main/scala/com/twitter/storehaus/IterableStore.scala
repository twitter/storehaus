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

import com.twitter.util.{ Future, Promise, Return }
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::

object IterableStore {
  /** Factory method to create a IterableStore from a Map. */
  def fromMap[K, V](m: Map[K, V]): IterableStore[K, V] = new MapStore(m)
}

/**
 * Trait for stores with iterator over their key-value pairs.
 *
 * Depending on the backing store, this may have performance implications. So use with caution.
 * In general, this should be okay to use with cache stores.
 * For other stores, the iterable should ideally be backed by a stream.
 */
trait IterableStore[K, V] extends ReadableStore[K, V] {

  protected def iteratorToSpool(it: Iterator[(K, V)]): Future[Spool[(K, V)]] = {
    val s = new Promise[Spool[(K, V)]]
    fillSpool(it, s)
    s
  }

  protected def fillSpool(it: Iterator[(K, V)], s: Promise[Spool[(K, V)]]): Unit = {
    if (it.isEmpty) {
      s() = Return(Spool.empty[(K, V)])
    } else {
      val next = new Promise[Spool[(K, V)]]
      s() = Return(it.next *:: next)
      fillSpool(it, next)
    }
  }

  def iterator: Future[Spool[(K, V)]]

  def withFilter(f: ((K, V)) => Boolean): Future[Spool[(K, V)]]
}

