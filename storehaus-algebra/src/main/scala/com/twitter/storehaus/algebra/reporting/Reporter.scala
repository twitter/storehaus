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

import com.twitter.storehaus.{Store, ReadableStore, WritableStore}
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.util.{Future, Time}

object Reporter {
  def sideEffect[T, U](
    params: U, f: Future[T],
    sideEffect: (U, Future[T]) => Future[Unit]
  ): Future[T] =
    Future.join(f, sideEffect(params, f)).map(_._1)

  def sideEffect[T, K, U](
    params: U, f: Map[K, Future[T]],
    sideEffect: (U, Map[K, Future[T]]) => Map[K, Future[Unit]]
  ): Map[K, Future[T]] = {
    val effected = sideEffect(params, f)
    f.map{case (k, v) =>
      val unitF = effected.getOrElse(k,
        sys.error("Reporter for multi side effect didn't return a future for key" + k.toString))
      (k, Future.join(v, unitF).map(_._1))
    }
  }
}

trait ReadableStoreReporter[S <: ReadableStore[K, V], K, V] extends ReadableStore[K, V] {
  def self: S
  override def get(k: K): Future[Option[V]] = Reporter.sideEffect(k, self.get(k), traceGet)
  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] =
    Reporter.sideEffect(keys, self.multiGet(keys), traceMultiGet)

  protected def traceMultiGet[K1 <: K](
    ks: Set[K1], request: Map[K1, Future[Option[V]]]): Map[K1, Future[Unit]]
  protected def traceGet(k: K, request: Future[Option[V]]): Future[Unit]
  override def close(time: Time): Future[Unit] = self.close(time)
}

trait WritableStoreReporter[S <: WritableStore[K, V], K, V] extends WritableStore[K, V] {
  def self: S
  override def put(kv: (K, V)): Future[Unit] = Reporter.sideEffect(kv, self.put(kv), tracePut)
  override def multiPut[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] =
    Reporter.sideEffect(kvs, self.multiPut(kvs), traceMultiPut)

  protected def tracePut(kv: (K, V), request: Future[Unit]): Future[Unit]
  protected def traceMultiPut[K1 <: K](
    kvs: Map[K1, V], request: Map[K1, Future[Unit]]): Map[K1, Future[Unit]]
  override def close(time: Time): Future[Unit] = self.close(time)
}


trait MergeableReporter[S <: Mergeable[K, V], K, V] extends Mergeable[K, V] {
  def self: S
  override def merge(kv: (K, V)): Future[Option[V]] =
    Reporter.sideEffect(kv, self.merge(kv), traceMerge)
  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] =
    Reporter.sideEffect(kvs, self.multiMerge(kvs), traceMultiMerge)

  protected def traceMerge(kv: (K, V), request: Future[Option[V]]): Future[Unit]
  protected def traceMultiMerge[K1 <: K](
    kvs: Map[K1, V], request: Map[K1, Future[Option[V]]]): Map[K1, Future[Unit]]
  override def close(time: Time): Future[Unit] = self.close(time)
}


trait StoreReporter[S <: Store[K, V], K, V]
    extends ReadableStoreReporter[S, K, V] with WritableStoreReporter[S, K, Option[V]] {
  def self: S
}
