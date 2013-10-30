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

package com.twitter.storehaus.algebra

import com.twitter.algebird.Monoid
import com.twitter.util.Future
import com.twitter.storehaus.{ AbstractReadableStore, ReadableStore }

trait StatReporter[K, V] {
  def traceGet(request: Future[Option[V]]): Future[Option[V]] = request
  def traceMultiGet[K1 <: K](request: Map[K1, Future[Option[V]]]): Map[K1, Future[Option[V]]] = request
  def getPresent {}
  def getAbsent {}
  def multiGetPresent {}
  def multiGetAbsent {}
}

case class ReadableStatStore[K, V](store: ReadableStore[K, V], reporter: StatReporter[K, V]) extends ReadableStore[K, V] {

  override def get(k: K): Future[Option[V]] =
    reporter.traceGet(store.get(k))
      .onSuccess {
        case Some(_) => reporter.getPresent
        case None => reporter.getAbsent
      }

  override def multiGet[K1 <: K](keys: Set[K1]) =
    reporter.traceMultiGet(store.multiGet(keys)).map { case (k, futureV) =>
      k -> futureV.onSuccess {
          case Some(_) => reporter.multiGetPresent
          case None => reporter.multiGetAbsent
        }
    }

}