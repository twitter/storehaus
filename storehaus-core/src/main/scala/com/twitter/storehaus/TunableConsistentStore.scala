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

/*
 * N - total replicas
 * One - 1 successful operation is sufficient to consider the operation as complete
 * Quorum - N/2 + 1 successful operations are sufficient to consider the operation as complete
 * All - N successful operations are required
 */
sealed trait ConsistencyLevel
case object One extends ConsistencyLevel
case object Quorum extends ConsistencyLevel
case object All extends ConsistencyLevel

/**
 * Replicates reads to a seq of stores and returns the value based on picked read consistency.
 *
 * Consistency semantics:
 * One - returns after first successful read, fails if all underlying reads fail
 * Quorum - returns after at least N/2 + 1 reads succeed and return the same value (read repairs divergent replicas), fails otherwise
 * All - returns if all N reads succeed and return the same value, fails otherwise
 */
class TunableConsistentReadableStore[-K, +V](stores: Seq[ReadableStore[K, V]], readConsistency: ConsistencyLevel)
  extends AbstractReadableStore[K, V] {
  override def get(k: K) = Future.None // TODO
  override def multiGet[K1 <: K](ks: Set[K1]) = ks.zipWithIndex.map{ case (k, v) => (k, Future.None) }.toMap // TODO
}

/**
 * Replicates writes to a seq of stores, and returns after picked write consistency is satisfied.
 *
 * Consistency semantics:
 * One - returns after first successful write (other writes can complete in the background), fails if no write is successful
 * Quorum - returns after N/2 + 1 writes succeed (other writes can complete in the background), fails otherwise
 * All - returns if all N writes succeed, fails otherwise
 *
 * For all write failures, the key is deleted on all replicas (in the background) as part of rollback.
 */
class TunableConsistentStore[-K, V](stores: Seq[Store[K, V]], readConsistency: ConsistencyLevel, writeConsistency: ConsistencyLevel)
  extends TunableConsistentReadableStore[K, V](stores, readConsistency)
  with Store[K, V] {
  override def put(kv: (K, Option[V])) = Future.None.unit // TODO
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) = collection.immutable.Map(kvs.map { case (k, v) => (k, Future.None.unit) }.toSeq: _*) // TODO
}
