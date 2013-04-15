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

import com.twitter.storehaus.{Store, FutureCollector, FutureOps}
import com.twitter.algebird.Monoid
import com.twitter.util.Future

/** A combinator to produce an inverted indexed as you are merging
 * TODO: this is not thread safe, gets and puts are separated.
 * we need to either add semantics to those APIs, or figure out
 * how to write all of these operations as merge
 */
object IndexedStore {
  // We totally own writes to store and index or shit busts, ONLY READ from the input stores after this call
  def index[K,V,R](store: Store[K,V], index: Store[R,Set[K]])(indexfn: V => Set[R])
    (implicit monv: Monoid[V], fc: FutureCollector[(R,Set[K])]): Mergeable[K,V] =
    new Mergeable[K,V] {
      override def monoid = monv
      override def merge(kv: (K,V)): Future[Unit] = {
        val (k,v) = kv
        store.get(k).flatMap { optv =>
          val oldRs = optv.map { indexfn(_) }.getOrElse(Set[R]())
          val newValue = Monoid.plus(optv, Some(v)).get
          val fPut = store.put((k, Monoid.nonZeroOption(newValue)))

          val newRs = indexfn(newValue)
          val oldNotNew = oldRs -- newRs
          val newNotOld = newRs -- oldRs
          val resultMap: Map[R, Future[Option[Set[K]]]] = index.multiGet(oldNotNew ++ newNotOld)
          val toPut: Map[R, Future[Set[K]]] = resultMap.map { case (r, fosk) =>
            if(oldNotNew(r)) {
              // remove from the set:
              (r, fosk.map { opt => opt.map { set => set - k }.getOrElse { Set[K]() } })
            }
            else {
              //in newNotOld
              (r, fosk.map { opt => opt.getOrElse(Set[K]()) + k })
            }
          }.toMap
          val inFuture: Future[Map[R, Set[K]]] = FutureOps.mapCollect(toPut)

          val indexPut = inFuture.flatMap { m =>
            val mres = index.multiPut(m.mapValues { s => if(s.isEmpty) None else Some(s) })
            FutureOps.mapCollect(mres).unit
          }
          Future.join(indexPut, fPut).unit
        }
      }
    }
}
