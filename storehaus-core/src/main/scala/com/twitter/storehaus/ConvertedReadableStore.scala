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

import com.twitter.util.{Future, Time}

/** Convert the keys/values of a store.
 *
 * Value conversion returns a Future because V1 => V2 may fail, and we
 * are going to convert to a future anyway (so, a Try is kind of a
 * Future that is not async). Thus we might as well add the
 * flexibility of accepting a V1 => Future[V2], though in practice
 * Future.value/exception will generally be used.
 */
class ConvertedReadableStore[K1, -K2, V1, +V2](rs: ReadableStore[K1, V1])(kfn: K2 => K1)(vfn: V1 => Future[V2])
  extends AbstractReadableStore[K2, V2] {
  override def get(k2: K2) = FutureOps.flatMapValue(rs.get(kfn(k2)))(vfn)
  override def multiGet[K3 <: K2](s: Set[K3]) = {
    val k1ToV2 = rs.multiGet(s.map(kfn)).mapValues(FutureOps.flatMapValue(_)(vfn))
    s.map { k3 => (k3, k1ToV2(kfn(k3))) }.toMap
  }
  override def close(time: Time) = rs.close(time)
}
