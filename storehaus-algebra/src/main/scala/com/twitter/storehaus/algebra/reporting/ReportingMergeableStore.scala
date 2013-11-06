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
import com.twitter.storehaus.algebra.MergeableStore

object ReportingMergeableStore {
  def apply[K, V](passedStore: MergeableStore[K, V],
  passedReporter: MergeableStoreReporter[K, V]) = new ReportingMergeableStore[K, V] {
    val store = passedStore
    val reporter = passedReporter
  }

}

trait ReportingMergeableStore[K, V] extends MergeableStore[K, V]
                                        with ReportingStore[K, V] with ReportingReadableStore[K,V] {
  def store: MergeableStore[K, V]
  def reporter: MergeableStoreReporter[K, V]
  lazy val semigroup = store.semigroup


  override def merge(kv: (K, V)) = Reporter.sideEffect(kv, store.merge(kv), reporter.traceMerge)
  override def multiMerge[K1 <: K](kvs: Map[K1, V]) = Reporter.sideEffect(kvs, store.multiMerge(kvs), reporter.traceMultiMerge)
}
