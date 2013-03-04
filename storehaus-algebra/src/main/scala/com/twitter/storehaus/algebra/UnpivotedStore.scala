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
import com.twitter.storehaus.{ FutureCollector, Store }

/**
 * Store enrichment which presents a Store[K, V] over top of a packed
 * Store[OuterK, Map[InnerK, V]].
 *
 * @author Sam Ritchie
 */

class UnpivotedStore[-K, OuterK, InnerK, V](store: Store[OuterK, Map[InnerK, V]])(split: K => (OuterK, InnerK))
  extends UnpivotedReadableStore[K, OuterK, InnerK, V](store)(split)
  with Store[K, V] {

  override def put(pair: (K, Option[V])) = {
    val (k, optV) = pair
    val (outerK, innerK) = split(k)
    store.get(outerK).map { mOpt: Option[Map[InnerK, V]] =>
      val optPair: Option[(InnerK, V)] = optV.map { innerK -> _ }
      (mOpt, optPair) match {
        case (Some(m), Some(pair)) => Some(m + pair)
        case (Some(m), None) => Some(m - innerK)
        case (None, Some(pair)) => Some(Map(pair))
        case (None, None) => None
      }
    }.foreach { newMap => store.put(outerK -> newMap) }.unit
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) =
    PivotOps.multiPut(store, kvs)(split)(FutureCollector.default)

  override def close { store.close }
}
