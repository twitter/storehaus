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
  * Returns a ReadableStore that attempts reads out of the supplied
  * Seq[ReadableStore] in order and returns the first non-exceptional
  * value that satisfies the supplied predicate.
  *
  * multiGet attempts a full multiGet of the first store, then follows
  * up on missing or failed keys with individual gets to the
  * underlying stores (as failures for each key can happen at
  * different times).
  */

class SearchingReadableStore[-K, +V](stores: Seq[ReadableStore[K, V]])(pred: Option[V] => Boolean) extends ReadableStore[K, V] {
  override def get(k: K) = FutureOps.find(stores.toStream.map(_.get(k)))(pred)
  override def multiGet[K1 <: K](ks: Set[K1]) = {
    val tail = stores.tail
    stores.head.multiGet(ks).map { case (k, v) =>
        k -> FutureOps.find(v #:: tail.toStream.map { _.get(k) })(pred)
    }
  }
}
