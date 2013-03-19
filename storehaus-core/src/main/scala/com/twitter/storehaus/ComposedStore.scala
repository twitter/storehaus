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
import java.io.Closeable

class ComposedStore[-K, V, V2, V3 >: V](l: ReadableStore[K, V], r: ReadableStore[V3, V2])(implicit fc: FutureCollector[V3])
  extends AbstractReadableStore[K, V2] {
  override def get(k: K) =
    for (
      optV <- l.get(k);
      v2 <- optV.map { v => r.get(v) }.getOrElse(Future.None)
    ) yield v2

  override def multiGet[K1 <: K](ks: Set[K1]) = {
    val mapFV: Map[K1, Future[Option[V]]] = l.multiGet(ks)
    val fkVals: Future[Map[K1, Option[V]]] = FutureOps.mapCollect(mapFV)

    val fmapf: Future[Map[K1, Future[Option[V2]]]] = fkVals.map { k1vMap =>
      val vSet: Set[V] = k1vMap.view.flatMap { _._2 }.toSet
      val v2s: Map[V, Future[Option[V2]]] = r.multiGet(vSet)
      k1vMap.mapValues { optV => optV.map { v2s(_) }.getOrElse(Future.None) }
    }
    FutureOps.liftValues(ks, fmapf).mapValues { _.flatten }
  }
  override def close { l.close; r.close }
}
