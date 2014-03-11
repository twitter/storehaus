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
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.FutureOps
import com.twitter.util.{ Future, Throw }

/**
  * @author Ruban Monu
  */

/**
  * Mergeable MySQL store that performs merge inside a transaction.
  */
class MergeableMySqlStore[V](underlying: MySqlStore)(implicit inj: Injection[V, MySqlValue],
    override val semigroup: Semigroup[V])
  extends ConvertedStore[MySqlValue, MySqlValue, MySqlValue, V](underlying)(identity)
  with MergeableStore[MySqlValue, V] {

  // Merges multiple keys inside a transaction.
  // 1. existing keys are fetched using multiGet (SELECT query)
  // 2. new keys are added using INSERT query
  // 3. existing keys are merged using UPDATE query
  // NOTE: merge on a single key also in turn calls this
  override def multiMerge[K1 <: MySqlValue](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    val mergeResult : Future[Map[K1, Option[V]]] = underlying.startTransaction.flatMap { u: Unit =>
      FutureOps.mapCollect(multiGet(kvs.keySet)).flatMap { result: Map[K1, Option[V]] =>
        val existingKeys = result.filter { !_._2.isEmpty }.keySet
        val newKeys = result.filter { _._2.isEmpty }.keySet

        // handle inserts for new keys
        val insertF = newKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val insertKvs = newKeys.map { k => k -> kvs.get(k).get }
            insertKvs.isEmpty match {
              case true => Future.Unit
              case false => underlying.executeMultiInsert(insertKvs.toMap.mapValues { v => inj(v) })
            }
        }

        // handle update/merge for existing keys
        // lazy val realized inside of insertF.flatMap
        lazy val updateF = existingKeys.isEmpty match {
          case true => Future.Unit
          case false =>
            val existingKvs = existingKeys.map { k => k -> kvs.get(k).get }
            underlying.executeMultiUpdate(existingKvs.map { kv =>
              val resV = semigroup.plus(result.get(kv._1).get.get, kv._2)
              kv._1 -> inj(resV)
            }.toMap)
        }

        // insert, update and commit or rollback accordingly
        insertF.flatMap { f =>
          updateF.flatMap { f =>
            underlying.commitTransaction.map { f =>
              // return values before the merge
              result
            }
          }
          .onFailure { case e: Exception =>
            underlying.rollbackTransaction.map { f =>
              // map values to exception
              result.mapValues { v => e }
            }
          }
        }
      }
    }
    FutureOps.liftValues(kvs.keySet, mergeResult)
  }
}

