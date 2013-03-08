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

import com.twitter.algebird.{ Monoid, Semigroup, StatefulSummer }
import com.twitter.util.{ Future, Promise }
import scala.collection.mutable.{ Map => MMap }
import com.twitter.storehaus.{ FutureCollector, Store }

class BufferingMergeable[-K, V](wrapped: Mergeable[K, V], summer: StatefulSummer[Map[K, V]]) extends Mergeable[K, V] {
  override implicit def monoid: Monoid[V] = wrapped.monoid

  private val promiseMap = MMap[Any, Promise[Unit]]()

  private def fuse[T](promise: Promise[T], future: Future[T]): Promise[T] = {
    promise.become(future)
    promise
  }

  override def merge(pair: (K, V)): Future[Unit] = multiMerge(Map(pair))(pair._1)

  protected def multiPromise[K1 <: K](ks: Set[K1]): Map[K1, Promise[Unit]] = {
    ks.map { k =>
      val newPromise = new Promise[Unit]
      val ret = promiseMap.get(k) match {
        case Some(promise) => fuse(promise, newPromise)
        case None => newPromise
      }
      promiseMap.update(k, ret)
      k -> ret
    }.toMap
  }

  protected def multiFulfill[K1 <: K](m: Map[K1, Future[Unit]]): Map[K1, Future[Unit]] =
    try {
      // TODO: What are the GC implications? If someone doesn't hold
      // onto a promise, is there a clean way to garbage collect?
      m.foreach { case (k, futureUnit) => k -> fuse(promiseMap(k), futureUnit) }
      m
    } finally {
      promiseMap --= m.keySet
    }

  protected def mergeAndFulfill[K1 <: K](m: Map[K1, V]) =
    multiFulfill(wrapped.multiMerge(m))

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = {
    val result: Map[K1, Future[Unit]] = multiPromise(kvs.keySet)
    summer.put(kvs.asInstanceOf[Map[K, V]]).foreach { mergeAndFulfill(_) }
    result
  }
}

class BufferingStore[-K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]])
  extends BufferingMergeable[K, V](store, summer)
  with MergeableStore[K, V] {

  protected def flush = summer.flush.foreach { store.multiMerge(_) }

  override def get(k: K): Future[Option[V]] =
    summer.flush
      .flatMap { mergeAndFulfill(_).get(k) }
      .getOrElse(Future.Unit)
      .flatMap { _ => store.get(k) }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    implicit val collector = FutureCollector.bestEffort[(K, Unit)]
    val writeComputation: Future[Unit] = summer.flush.map { m =>
      Store.mapCollect {
        mergeAndFulfill(m).filterKeys(ks.toSet[K])
      }.unit
    }.getOrElse(Future.Unit)
    Store.liftFutureValues(ks, writeComputation.map { _ => store.multiGet(ks) })
  }

  override def put(pair: (K, Option[V])) = { flush; store.put(pair) }
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = { flush; store.multiPut(kvs) }
}
