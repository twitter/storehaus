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

/** This is only thrown when a value is expected but somehow absent.
 * For instance, in the combinators, we expect all the underlying gets to behave correctly
 * this exception is used when a dependent store is not returning correct data
 */
class MissingValueException[K](val key: K)
    extends RuntimeException("Missing value for " + key)

object Store {
  def flatMapValue[V,V2](f: Future[Option[V]])(fn: V => Future[V2]): Future[Option[V2]] =
    f.flatMap { _ match {
        case Some(v) => fn(v).map { v2 => Some(v2) }
        case None => Future.None
      }
    }

  // TODO: Move to some collection util.
  def zipWith[K, V](keys: Iterable[K])(lookup: K => V): Map[K, V] =
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
    fc(m.view.map { case (k,fv) => fv.map { v => (k,v) } }.toSeq).map { _.toMap }

  def missingValueFor[K](k: K) = Future.exception(new MissingValueException(k))

  def liftValues[K,K1<:K,V](ks: Set[K1], result: Future[Map[K, V]],
    missingfn: (K1) => Future[V] = missingValueFor _): Map[K1, Future[V]] =
    ks.view.map { k1 =>
      k1 -> result.flatMap { _.get(k1).map { Future.value(_) }.getOrElse(missingfn(k1)) }
    }.toMap
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
