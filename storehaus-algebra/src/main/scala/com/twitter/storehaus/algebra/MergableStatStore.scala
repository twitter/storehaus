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

import com.twitter.util.Future
import com.twitter.storehaus.Store

object MergableStatStore {
  def apply[K, V](store: MergeableStore[K, V],
  reporter: StatReporter[K, V]) = new MergableStatStore(store, reporter)
}

class MergableStatStore[K, V](
  store: MergeableStore[K, V],
  reporter: StatReporter[K, V])
    extends ReadableStatStore[K, V](store, reporter)
    with MergeableStore[K, V] {

  val semigroup = store.semigroup

  override def put(kv: (K, Option[V])): Future[Unit] = {
    val f = reporter.tracePut(store.put(kv))
    kv._2 match {
      case Some(_) => f.onSuccess(_ => reporter.putSome)
      case None => f.onSuccess(_ => reporter.putNone)
    }
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) =
    reporter.traceMultiPut(store.multiPut(kvs)).map { case (k, futureUnit) =>
      k -> futureUnit.onSuccess { _ =>
          kvs(k) match {
            case Some(_) => reporter.multiPutSome
            case None => reporter.multiPutNone
          }
        }
    }

  override def merge(kv: (K, V)): Future[Option[V]] =
    reporter.traceMerge(store.merge(kv)).onSuccess{
        case Some(_) => reporter.mergeWithSome
        case None => reporter.mergeWithNone
      }

  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] =
    reporter.traceMultiMerge(store.multiMerge(kvs)).map { case (k, futureOldV) =>
        k -> futureOldV.onSuccess {
            case Some(_) => reporter.multiMergeWithSome
            case None => reporter.multiMergeWithNone
          }
      }


}