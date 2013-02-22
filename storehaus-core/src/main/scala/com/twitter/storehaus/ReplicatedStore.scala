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

import Store.{selectFirstSuccessfulTrial => selectFirst}
import ReadableStore.combineMaps

class ReplicatedReadableStore[-K, +V](stores: Seq[ReadableStore[K, V]]) extends ReadableStore[K, V]
{
  override def get(k: K) = selectFirst(stores.map { _.get(k) })
  override def multiGet[K1<:K](ks: Set[K1]) =
    combineMaps(stores.map { _.multiGet(ks) }).mapValues { selectFirst(_) }
}

/**
 * Replicates writes to all stores, and takes the first successful read.
 */
class ReplicatedStore[-K, V](stores: Seq[Store[K, V]])(implicit collect: FutureCollector[Unit])
extends Store[K, V] {
  override def get(k: K) = selectFirst(stores.map { _.get(k) })
  override def multiGet[K1<:K](ks: Set[K1]) =
    combineMaps(stores.map { _.multiGet(ks) }).mapValues { selectFirst(_) }
  override def put(kv: (K,Option[V])) =
    collect(stores.map { _.put(kv) }).map { _ => () }
  override def multiPut[K1<:K](kvs: Map[K1, Option[V]]) =
    combineMaps(stores.map { _.multiPut(kvs) })
      .mapValues { seqf => collect(seqf).map { _ => () } }
}
