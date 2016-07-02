/*
 * Copyright 2014 Twitter Inc.
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
import com.twitter.util.Future

/**
 * @author Mansur Ashraf
 * @since 1/22/14
 */
class EnrichedQueryableStore[-K, V, Q](store: Store[K, V] with QueryableStore[Q, V])
  extends EnrichedStore[K, V](store) {
  // Converts a QueryableStore
  override def convert[K2, V2](kfn: K2 => K)(implicit inj: Injection[V2, V]): Store[K2, V2]
      with QueryableStore[Q, V2] =
    new ConvertedStore[K, K2, V, V2](store)(kfn)(inj) with QueryableStore[Q, V2] {
      def queryable: ReadableStore[Q, Seq[V2]] = store.queryable.convert[Q, Seq[V2]](identity[Q]) {
        v => Future {
          v.map {
            inj.invert(_).get
          }
        }
      }
    }

  override def mapValues[V1](implicit inj: Injection[V1, V]): Store[K, V1]
      with QueryableStore[Q, V1] =
    convert[K, V1](identity[K])
}
