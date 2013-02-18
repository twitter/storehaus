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

import com.twitter.util.Future
import com.twitter.algebird.{Monoid, Semigroup}

import Store.{selectFirstSuccessfulTrial => selectFirst}

/**
 * Replicates writes to all stores, and takes the first successful read.
 */
class ReplicatedStore[K, V](stores: Seq[MergeableStore[K,V]])
  (implicit collect: FutureCollector[V], override val semigroup: Semigroup[V])
extends MergeableStore[K, V] {

  override def get(k: K) = selectFirst(stores.map { _.get(k) })
  override def multiGet(ks: Set[K]) = {
    Store.sequenceMap(stores.map { _.multiGet(ks) })
      .mapValues { selectFirst(_) }
  }

  protected def selector(res: Seq[V]): V = {
    // How do return a value here? several strategies.
    // TODO
    res.head
  }
  override def add(kv: (K,V)) = collect(stores.map { _.add(kv) }).map { selector(_) }
  override def multiAdd(kvs: Map[K,V]) ={
    val res: Map[K,Future[Seq[V]]] = FutureCollector.mapCollect(stores.map { _.multiAdd(kvs) })
    res.mapValues { f => f.map { selector(_) } }
  }
}
