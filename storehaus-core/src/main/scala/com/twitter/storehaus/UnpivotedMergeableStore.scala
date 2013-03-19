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

/**
 * MergeableStore enrichment which presents a MergeableStore[K, V]
 * over top of a packed MergeableStore[OuterK, Map[InnerK, V]].
 *
 * @author Sam Ritchie
 */

class UnpivotedMergeableStore[-K, OuterK, InnerK, V](store: MergeableStore[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK))
  extends UnpivotedStore[K, OuterK, InnerK, V](store)(split)
  with MergeableStore[K, V] {
  protected val unpivotedMergeable = new UnpivotedMergeable(store)(split)
  override def merge(pair: (K, V)): Future[Unit] = unpivotedMergeable.merge(pair)
  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = unpivotedMergeable.multiMerge(kvs)
  override def close { store.close }
}
