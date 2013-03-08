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
 * Helpful transformations on Futures. These should arguably exist in
 * util-core.
 */

/**
 * This is only thrown when a value is expected but somehow absent.
 * For instance, in the combinators, we expect all the underlying gets
 * to behave correctly this exception is used when a dependent store
 * is not returning correct data.
 */
class MissingValueException[K](val key: K) extends RuntimeException("Missing value for " + key)

object FutureOps {
  def missingValueFor[K](k: K) = Future.exception(new MissingValueException(k))

  // Kleisli operator for Future[Option[_]] Monad.  I knew it would come to this.
  def combineFOFn[A, B, C](f1: A => Future[Option[B]], f2: B => Future[Option[C]])(a: A): Future[Option[C]] = {
    f1(a).flatMap { optB =>
      optB match {
        case None => Future.None
        case Some(b) => f2(b)
      }
    }
  }

  def flatMapValue[V, V2](f: Future[Option[V]])(fn: V => Future[V2]): Future[Option[V2]] =
    f.flatMap {
      _ match {
        case Some(v) => fn(v).map { v2 => Some(v2) }
        case None => Future.None
      }
    }

  def selectFirstSuccessfulTrial[T](futures: Seq[Future[T]]): Future[T] =
    Future.select(futures)
      .flatMap { case (completedTry, otherFutures) =>
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

  def mapCollect[K, V](m: Map[K, Future[V]])(implicit fc: FutureCollector[(K, V)]): Future[Map[K, V]] =
    fc(m.view.map { case (k, fv) => fv.map { v => (k, v) } }.toSeq).map { _.toMap }

  def liftValues[K, K1 <: K, V](ks: Set[K1], result: Future[Map[K, V]],
    missingfn: (K1) => Future[V] = missingValueFor _): Map[K1, Future[V]] =
    ks.view.map { k1 =>
      k1 -> result.flatMap { _.get(k1).map { Future.value(_) }.getOrElse(missingfn(k1)) }
    }.toMap

  def liftFutureValues[K, K1 <: K, V](ks: Set[K1], result: Future[Map[K, Future[V]]],
    missingfn: (K1) => Future[V] = missingValueFor _): Map[K1, Future[V]] =
    ks.view.map { k1 =>
      k1 -> result.flatMap { _.get(k1).getOrElse(missingfn(k1)) }
    }.toMap
}
