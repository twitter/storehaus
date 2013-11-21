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

import com.twitter.bijection.Injection
import com.twitter.util.{ Future, Time }

import scala.util.{ Failure, Success }

/**
 * ReadableStore enrichment for ReadableStore[OuterK, ReadableStore[InnerK, V]]
 * on top of a ReadableStore[K, V]
 *
 * @author Ruban Monu
 */
object PivotedReadableStore {

  def fromMap[K, OuterK, InnerK, V](m: Map[K, V])(implicit inj: Injection[(OuterK, InnerK), K]) =
    new PivotedReadableStore[K, OuterK, InnerK, V](ReadableStore.fromMap(m))(inj)

  def fromReadableStore[K, OuterK, InnerK, V](store: ReadableStore[K, V])(implicit inj: Injection[(OuterK, InnerK), K]) =
    new PivotedReadableStore[K, OuterK, InnerK, V](store)(inj)
}

class PivotedReadableStore[K, -OuterK, InnerK, +V](store: ReadableStore[K, V])(implicit inj: Injection[(OuterK, InnerK), K])
    extends ReadableStore[OuterK, ReadableStore[InnerK, V]] {

  override def get(outerK: OuterK) : Future[Option[ReadableStore[InnerK, V]]] =
    Future.value(Some(new ReadableStore[InnerK, V]() {
      override def get(innerK: InnerK) = store.get(inj((outerK, innerK)))
    }))

  override def close(time: Time) = store.close(time)
}

