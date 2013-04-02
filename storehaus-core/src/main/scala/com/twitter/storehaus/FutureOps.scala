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

import com.twitter.util.{ Future, Throw, Return }

/**
 * This is only thrown when a value is expected but somehow absent.
 * For instance, in the combinators, we expect all the underlying gets
 * to behave correctly this exception is used when a dependent store
 * is not returning correct data.
 */
class MissingValueException[K](val key: K) extends RuntimeException("Missing value for " + key)

/** Some combinators on Futures or Seqs of Futures that are used internally
 * These should arguably exist in util-core.
 */
object FutureOps {
  def missingValueFor[K](k: K) = Future.exception(new MissingValueException(k))

  /** Kleisli operator for Future[Option[_]] Monad.  I knew it would come to this. */
  def combineFOFn[A, B, C](f1: A => Future[Option[B]], f2: B => Future[Option[C]])(a: A): Future[Option[C]] = {
    f1(a).flatMap { optB =>
      optB match {
        case None => Future.None
        case Some(b) => f2(b)
      }
    }
  }

  /** If the f has Future(None) return None, otherwise flatMap the value with the given function */
  def flatMapValue[V, V2](f: Future[Option[V]])(fn: V => Future[V2]): Future[Option[V2]] =
    f.flatMap {
      _ match {
        case Some(v) => fn(v).map { v2 => Some(v2) }
        case None => Future.None
      }
    }

  /** Given a Seq of equivalent Futures, return the first
    * non-exceptional that passes the supplied predicate. If all
    * futures fail to succeed or pass the predicate, the final future
    * to complete will be returned. */
  def selectFirstSuccessfulTrial[T](futures: Seq[Future[T]])(pred: T => Boolean): Future[T] =
    Future.select(futures)
      .flatMap { case (completedTry, otherFutures) =>
        if (otherFutures.isEmpty)
          Future.const(completedTry)
        else
          completedTry.filter(pred) match {
            case Throw(e) => selectFirstSuccessfulTrial(otherFutures)(pred)
            case Return(t) => Future.value(t)
          }
    }

  /**
    * Given a Future, a lazy reference to a sequence of remaining
    * futures and a predicate on T, tries each future in order and
    * returns the first Future to both succeed and pass the supplied
    * predicate.
    */
  def find[T](futures: Stream[Future[T]])(pred: T => Boolean): Future[T] =
    futures match {
      case Stream.Empty => Future.exception(new RuntimeException("Empty iterator in FutureOps.find"))
      case last #:: Stream.Empty => last
      case next #:: rest => next.filter(pred).rescue { case _: Throwable => find(rest)(pred) }
    }

  /** Use the given future collector to produce a single Future of Map from a Map with Future values */
  def mapCollect[K, V](m: Map[K, Future[V]])(implicit fc: FutureCollector[(K, V)]): Future[Map[K, V]] =
    fc(m.view.map { case (k, fv) => fv.map { v => (k, v) } }.toSeq).map { _.toMap }

  /** remove the outer wrapping of a Future and push it onto the values. */
  def liftValues[K, K1 <: K, V](ks: Set[K1], result: Future[Map[K, V]],
    missingfn: (K1) => Future[V] = missingValueFor _): Map[K1, Future[V]] =
    ks.view.map { k1 =>
      k1 -> result.flatMap { _.get(k1).map { Future.value(_) }.getOrElse(missingfn(k1)) }
    }.toMap

  /** Push the outer future on a result into all the values
   * Useful for putting finagle memcache into the same API as storehaus.
   */
  def liftFutureValues[K, K1 <: K, V](ks: Set[K1], result: Future[Map[K, Future[V]]],
    missingfn: (K1) => Future[V] = missingValueFor _): Map[K1, Future[V]] =
    ks.view.map { k1 =>
      k1 -> result.flatMap { _.get(k1).getOrElse(missingfn(k1)) }
    }.toMap
}
