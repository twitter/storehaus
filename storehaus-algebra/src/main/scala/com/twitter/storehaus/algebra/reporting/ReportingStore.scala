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
import com.twitter.storehaus.Store

object ReportingStore {
  def apply[K, V](passedStore: Store[K, V],
  passedReporter: StoreReporter[K, V]) = new ReportingStore[K, V] {
    val store = passedStore
    val reporter = passedReporter
  }

}

trait ReportingStore[K, V] extends Store[K, V] {
  def store: Store[K, V]
  def reporter: StoreReporter[K, V]

  override def put(kv: (K, Option[V])): Future[Unit] = Reporter.sideEffect(kv, store.put(kv), reporter.tracePut)
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = Reporter.sideEffect(kvs, store.multiPut(kvs), reporter.traceMultiPut)
}