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

class BufferingMergeable[-K, V](wrapped: Mergeable[K, V], summer: StatefulSummer[Map[K, V]]) extends Mergeable[K, V] {
  override implicit def monoid: Monoid[V] = wrapped.monoid

  private def promiseMap[K1 <: K] = MMap[K1, Promise[Unit]]()
  private def fuse[T](promise: Promise[T], future: Future[T]): Promise[T] = {
    promise.become(future)
    promise
  }

  override def merge(pair: (K, V)): Future[Unit] = multiMerge(Map(pair))(pair._1)

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = {
    summer.put(kvs.asInstanceOf[Map[K, V]])
      .map { m =>
        try {
          wrapped.multiMerge(m).map {
            case (k, futureUnit) =>
              k -> fuse(promiseMap(k), futureUnit)
          }.asInstanceOf[Map[K1, Future[Unit]]]
        } finally {
          promiseMap[K1].clear
        }
      }
      .getOrElse {
        kvs.map {
          case (k, _) =>
            val newPromise = new Promise[Unit]
            val ret = promiseMap.get(k) match {
              case Some(promise) => fuse(promise, newPromise)
              case None => newPromise
            }
            promiseMap.update(k, ret)
            k -> ret
        }
      }
  }
}

class BufferingStore[-K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]])
  extends BufferingMergeable[K, V](store, summer)
  with MergeableStore[K, V] {

  protected def flush = summer.flush.foreach { store.multiMerge(_) }

  protected def selectAndSum[K1 <: K](pair: (K1, Option[V]), m: Option[Map[K1, V]]): Option[V] =
    m.flatMap { partials =>
      Semigroup.plus(pair._2, partials.get(pair._1))
    }

  override def get(k: K): Future[Option[V]] = {
    val flushed = summer.flush
    try {
      store.get(k).map { v => selectAndSum((k, v), flushed) }
    } finally {
      flushed.foreach { store.multiMerge(_) }
    }

  }
  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val flushed = summer.flush
    try {
      store.multiGet(ks).map {
        case (k, futureV) =>
          k -> futureV.map { v => selectAndSum((k, v), flushed) }
      }
    } finally {
      flushed.foreach { store.multiMerge(_) }
    }
  }

  override def put(pair: (K, Option[V])) = { flush; store.put(pair) }
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = { flush; store.multiPut(kvs) }
}
