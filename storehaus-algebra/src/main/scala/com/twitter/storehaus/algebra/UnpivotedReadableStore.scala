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

import com.twitter.algebird.Monoid
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.bijection.Pivot
import com.twitter.util.Future
import com.twitter.storehaus.ReadableStore

/**
 * ReadableStore enrichment which presents a ReadableStore[K, V] over
 * top of a packed ReadableStore[OuterK, Map[InnerK, V]].
 *
 * @author Sam Ritchie
 */

class UnpivotedReadableStore[K, OuterK, InnerK, V](store: ReadableStore[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK))
  extends ReadableStore[K, V] {
  override def get(k: K) =
    PivotOps.get(k)(split) { store.get(_) }

  override def multiGet[T <: K](ks: Set[T]): Map[T, Future[Option[V]]] =
    PivotOps.multiGet(ks)(split) { store.multiGet(_) }

  override def close { store.close }
}
