/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.elasticsearch

import com.twitter.bijection.Injection
import com.twitter.util.Future
import com.twitter.storehaus.ConvertedStore

/**
 * @author Mansur Ashraf
 * @since 1/14/14
 */
class QueryableConvertedStore[K1, -K2, V1, V2, Q](store: QueryableStore[K1, V1, Q])(kfn: K2 => K1)(implicit inj: Injection[V2, V1]) extends ConvertedStore[K1, K2, V1, V2](store)(kfn) with QueryableStore[K2, V2, Q] {
  def query(query: Q): Future[Option[Seq[V2]]] = {
    val future = store.query(query)
    future.map {
      opt =>
        opt.map {
          seq =>
            seq.map {
              v => inj.invert(v).get
            }
        }
    }
  }
}

object QueryableConvertedStore {
  def convert[K1, K2, V1, V2, Q](store: QueryableStore[K1, V1, Q])(kfn: K2 => K1)(implicit inj: Injection[V2, V1]) = new QueryableConvertedStore[K1, K2, V1, V2, Q](store)(kfn)
}
