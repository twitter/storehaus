/*
 * Copyright 2014 Twitter Inc.
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

import com.twitter.util.{ Future, Promise }
import com.twitter.storehaus.cache.Atomic
import scala.collection.breakOut
/**
 * Instead of doing a get, this combinator always does a multiGet after
 * enough gets have been created.
 *
 * @param self the store to fetch values from
 * @param minMultiGetSize a multiGet to `store` will fetch values for at most `maxMultiGetSize` keys
 */
class GetBatchingReadableStore[K, V](
    override protected val self: ReadableStore[K, V],
    minMultiGetSize: Int)
    (implicit fc: FutureCollector[(K, V)]) extends ReadableStoreProxy[K, V] {

  private val empty = (0, Nil)
  /** Keep the size of pending gets, and the list of keys
   * we keep the size because we want O(1) cost.
   */
  private val toGet = Atomic[(Int, List[(K, Promise[Option[V]])])](empty)

  /** Execute a multiget now.
   * This future completes when all of the multiGets are done
   */
  def flush: Future[Unit] =
    toGet.effect { ks =>
      if (ks._2.isEmpty) (None, empty)
      else (Some(ks._2), empty)
    }
    ._1 match {
      case None => Future.Unit
      case Some(ks) => FutureOps.mapCollect(doMulti(ks)).unit
    }

  private def doMulti(items: Iterable[(K, Promise[Option[V]])]): Map[K, Future[Option[V]]] = {
    // time to run:
    val called = self.multiGet(items.iterator.map(_._1).toSet)
    items.foreach { case (k, p) => p.become(called(k)) }
    called
  }

  override def get(k: K) = {
    val res = new Promise[Option[V]]()
    toGet.effect { case (size, pending) =>
      val next = (k, res) :: pending
      val newSize = size + 1
      if (newSize >= minMultiGetSize) {
        // time to run:
        (Some(next), empty)
      }
      else {
        // Wait for another get
        (None, (newSize, next))
      }
    }
    ._1
    .foreach(doMulti(_))
    // always return the promise
    res
  }

  /** Always go through get so we don't leave keys sitting around too long
   */
  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] =
    keys.map { k => (k, get(k)) }(breakOut)
}
