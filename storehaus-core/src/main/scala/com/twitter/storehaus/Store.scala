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

import com.twitter.util.{Future, Throw, Return}
import java.io.Closeable

object Store {
  // TODO: Move to some collection util.
  def zipWith[K, V](keys: Set[K])(lookup: K => V): Map[K, V] =
    keys.foldLeft(Map.empty[K, V]) { (m, k) => m + (k -> lookup(k)) }

  def selectFirstSuccessfulTrial[T](futures: Seq[Future[T]]): Future[T] = {
    Future.select(futures).flatMap(entry => {
      val (completedTry, otherFutures) = entry
      completedTry match {
        case Throw(e) => {
          if (otherFutures.isEmpty) {
            Future.exception(e)
          } else {
            selectFirstSuccessfulTrial(otherFutures)
          }
        }
        case Return(similarUsers) => Future.value(similarUsers)
      }
    })
  }
  def mapCollect[K, V](m: Map[K, Future[V]])(implicit fc: FutureCollector[(K,V)]): Future[Map[K,V]] =
    fc(m.toSeq.map { case (k,fv) => fv.map { v => (k,v) } }).map { _.toMap }
}

trait Store[-K, V] extends ReadableStore[K, V] with Closeable { self =>

  /** replace a value
   * Delete is the same as put((k,None))
   */
  def put(kv: (K,Option[V])): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
  def multiPut[K1<:K](kvs: Map[K1,Option[V]]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, put(kv)) }

  override def close { }
}
