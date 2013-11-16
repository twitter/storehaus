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
import com.twitter.util.Future

/**
 * Store enrichment for Store[OuterK, Map[InnerK, V]] on top of a Store[K, V]
 *
 * @author Ruban Monu
 */
class PivotedStore[K, -OuterK, InnerK, V](store: IterableStore[K, V])(bij: Bijection[(OuterK, InnerK), K])
    extends PivotedReadableStore[K, OuterK, InnerK, V](store)(bij)
    with Store[OuterK, Map[InnerK, V]] {

  override def put(pair: (OuterK, Option[Map[InnerK, V]])) = {
    val (outerK, optMap) = pair
    optMap match {
      case Some(m) => Future.collect(m.map { case (innerK, v) => store.put((bij(outerK, innerK), Some(v))) }.toSeq).unit
      case None => store.getAllWithFilter({ k: K => bij.invert(k)._1 == outerK }).flatMap { case kvIter =>
        Future.collect(kvIter.map { case kv => store.put((kv._1, None))}.toSeq).unit
      }
    }
  }

  override def multiPut[OuterK1 <: OuterK](kvs: Map[OuterK1, Option[Map[InnerK, V]]]) =
    kvs.map { case (outerK, optMap) => outerK -> put((outerK, optMap)) }
}

