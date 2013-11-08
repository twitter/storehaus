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

package com.twitter.storehaus.algebra.reporting

import com.twitter.util.Future

object Reporter {
  def sideEffect[T, U](params: U, f: Future[T], sideEffect: (U, Future[T]) => Future[Unit]): Future[T] =
        Future.join(f, sideEffect(params, f)).map(_._1)

  def sideEffect[T, K, U](params: U, f: Map[K, Future[T]], sideEffect: (U, Map[K, Future[T]]) => Map[K, Future[Unit]]): Map[K, Future[T]] = {
        val effected = sideEffect(params, f)
        f.map{case (k, v) =>
            val unitF = effected.getOrElse(k, sys.error("Reporter for multi side effect didn't return a future for key" + k.toString))
            (k, Future.join(v, unitF).map(_._1))
        }
      }

}


trait ReadableStoreReporter[K, V]{
  def traceMultiGet[K1 <: K](ks: Set[K1], request: Map[K1, Future[Option[V]]]) = request.mapValues(_.unit)
  def traceGet(k: K, request: Future[Option[V]]) = request.unit
}

trait WritableStoreReporter[K, V] {
  def tracePut(kv: (K, Option[V]), request: Future[Unit]) = request
  def traceMultiPut[K1 <: K](kvs: Map[K1, Option[V]], request: Map[K1, Future[Unit]]): Map[K1, Future[Unit]] = request
}

trait StoreReporter[K, V] extends ReadableStoreReporter[K, V] with WritableStoreReporter[K, V]

trait MergeableStoreReporter[K, V] extends StoreReporter[K, V]{
  def traceMerge(kv: (K, V), request: Future[Option[V]]) = request.unit
  def traceMultiMerge[K1 <: K](kvs: Map[K1, V], request: Map[K1, Future[Option[V]]]) = request.mapValues(_.unit)
}
