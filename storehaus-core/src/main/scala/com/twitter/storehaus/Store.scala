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

import com.twitter.util.{Duration, Future, Throw, Return, Timer}
import java.io.Closeable

object ReadableStore {
  def empty[K,V]: ReadableStore[K,V] = new EmptyReadableStore[K, V]
}

trait ReadableStore[K,V] extends Closeable { self =>
  def get(k: K): Future[Option[V]] = multiGet(Set(k)).flatMap { _.apply(k) }

  /**
   * all keys in the set are in the resulting map
   */
  def multiGet(ks: Set[K]): Future[Map[K,Future[Option[V]]]] =
    Future(ks.map { k => (k, get(k)) }.toMap)

  /**
   * Returns a new ReadableStore[K, V] that queries both this and the other
   * ReadableStore[K,V] for get and multiGet and returns the first
   * future to succeed (following com.twitter.util.Future's "or" logic).
   */
  def or(other: ReadableStore[K,V]) = {
    import Store.{selectFirstSuccessfulTrial => selectFirst}
    new ReadableStore[K,V] {
      override def get(k: K) = selectFirst(Seq(self.get(k), other.get(k)))
      override def multiGet(ks: Set[K]) = selectFirst(Seq(self.multiGet(ks), other.multiGet(ks)))
    }
  }

  override def close { }
}

// Store is immutable by default.

object Store {
  // TODO: Move to some collection util.
  def zipWith[K, V](keys: Set[K])(lookup: K => V): Map[K, V] =
    keys.foldLeft(Map.empty[K, V]) { (m, k) => m + (k -> lookup(k)) }

  def selectFirstSuccessfulTrial[T](futures: Seq[Future[T]]): Future[T] =
    Future.select(futures).flatMap { case (completedTry, otherFutures) =>
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
    }

  protected def assignTimeout[T](f: Future[T], timeout: Duration)(implicit timer: Timer): Future[T] =
    timeout match {
      case Duration.forever => f
      case other => f.within(other)
    }

  def bestEffortCollect[T](futureOpts: Seq[Future[Option[T]]], timeout: Duration)
  (implicit timer: Timer): Future[Seq[Option[T]]] =
    Future.collect {
      futureOpts.map { f: Future[Option[T]] =>
        assignTimeout(f, timeout)
          .map { Some(_) }
          .handle { case _ => None }
      }
    }.map { _.flatten }
}


trait Store[Self <: Store[Self,K,V], K, V] extends ReadableStore[K, V] {
  def -(k: K): Future[Self] = this.update(k) { _ => Future.None }
  def +(pair: (K,V)): Future[Self] =  this.update(pair._1) { _ => Future.value(Some(pair._2)) }

  // Alias for "+".
  def set(pair: (K,V)): Future[Self] = this + pair

  def multiSet(m: Map[K, Future[Option[V]]]): Future[Self] =
    multiUpdate(m.mapValues { v => { _ => v } })

  private def submit(k: K, optV: Option[V]): Future[Self] =
    optV.map { v => this + (k -> v) }.getOrElse(this - k)

  def update(k: K)(fn: Option[V] => Future[Option[V]]): Future[Self] =
    for (opt <- get(k);
         nextOpt <- fn(opt);
         result <- submit(k, nextOpt))
    yield result

  def multiUpdate(m: Map[K, Option[V] => Future[Option[V]]]): Future[Self] =
    m.foldLeft(Future.value(this.asInstanceOf[Self])) { case (futureStore, (k, optFn)) =>
      futureStore.flatMap { _.update(k)(optFn) }
    }
}

object KeysetStore {
  def fromMap[K,V](m: Map[K,V]): KeysetStore[MapStore[K,V],K,V] = new MapStore(m)
}

trait KeysetStore[Self <: KeysetStore[Self,K,V],K,V] extends Store[Self,K,V] {
  def keySet: Set[K]
  def size: Int
}

// Used as a constraint annotation.
trait MutableStore[Self <: MutableStore[Self,K,V],K,V] extends Store[Self,K,V]

// Used as a constraint annotation.
trait ConcurrentMutableStore[Self <: ConcurrentMutableStore[Self,K,V],K,V] extends MutableStore[Self,K,V]
