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

import com.twitter.bijection.Bijection
import com.twitter.util.{ Future, Time }

/**
 * ReadableStore enrichment for ReadableStore[OuterK, Map[InnerK, V]]
 * on top of a ReadableStore[K, V]
 *
 * @author Ruban Monu
 */
class PivotedReadableStore[K, -OuterK, InnerK, +V](store: IterableReadableStore[K, V])(bij: Bijection[(OuterK, InnerK), K])
    extends ReadableStore[OuterK, Map[InnerK, V]] {

  override def get(outerK: OuterK) : Future[Option[Map[InnerK, V]]] =
    store.getAllWithFilter({ k: K => bij.invert(k)._1 == outerK }).map { case kvIter =>
      Some(kvIter.map { case kv =>
        val (outerK, innerK) = bij.invert(kv._1)
        (innerK -> kv._2)
      }.toMap)
    }
    // any semantic diff between Future.None and Future(Map.empty)?

  override def multiGet[T <: OuterK](ks: Set[T]): Map[T, Future[Option[Map[InnerK, V]]]] =
    ks.map { case k => k -> get(k) }.toMap
    // TODO: call getAll once and reuse for each key in ks

  override def close(time: Time) = store.close(time)
}

