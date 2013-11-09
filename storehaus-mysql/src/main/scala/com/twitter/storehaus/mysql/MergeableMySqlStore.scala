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

package com.twitter.storehaus.mysql

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.finagle.exp.mysql.Client
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{ Future, Throw }

/**
  * @author Ruban Monu
  */

/**
  * Mergeable MySQL store that performs merge inside a transaction.
  */
abstract class MergeableMySqlStore[V](underlying: MySqlStore)(implicit inj: Injection[V, MySqlValue])
  extends ConvertedStore[MySqlValue, MySqlValue, MySqlValue, V](underlying)(identity)
  with MergeableStore[MySqlValue, V] {

  override def merge(kv: (MySqlValue, V)): Future[Option[V]] = {
    underlying.startTransaction.flatMap { u: Unit =>
      get(kv._1).flatMap { optV: Option[V] =>
        val incV = kv._2
        val resV = optV.map(semigroup.plus(_, incV)).orElse(Some(incV))

        put((kv._1, resV)).flatMap { u: Unit =>
          underlying.commitTransaction.flatMap { u: Unit => Future.value(optV) }
            .onFailure { case e: Exception =>
              underlying.rollbackTransaction.flatMap { u: Unit => Future.exception(e) }
            }
        }
      }
    }
  }
}

