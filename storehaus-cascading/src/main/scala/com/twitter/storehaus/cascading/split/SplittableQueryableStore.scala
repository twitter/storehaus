/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.storehaus.cascading.split

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{ QueryableStore, ReadableStore, ReadableStoreProxy }
import com.twitter.util.{ Await, Future }
import com.twitter.concurrent.Spool.{*::}
import org.apache.hadoop.io.Writable

class SplittableQueryableStore[K, V, Q <: Writable, U <: QueryableStore[Q, (K, V)] with ReadableStore[K, V]]
		(store: U, splittingFunction: (Q, Int, Option[Long]) => Seq[Q], val query: Q, val version: Option[Long] = None) 
    extends SplittableStore[K, V, Q] 
    with QueryableStore[Q, (K, V)] 
    with ReadableStoreProxy[K, V] {

  override def self = store

  def getWritable: Q = query
  
  override def getSplits(numberOfSplitsHint: Int): Seq[SplittableStore[K, V, Q]] = {
    val queries = splittingFunction(query, numberOfSplitsHint, version)
    queries.map(qu => getSplit(qu, version))
  }

  override def getSplit(predicate: Q, version: Option[Long]): SplittableStore[K, V, Q] = {
    new SplittableQueryableStore[K, V, Q, U](store, splittingFunction, predicate, version).asInstanceOf[SplittableQueryableStore[K, V, Q, U]]
  }

  override def getAll: Spool[(K, V)] = {
    val seq = Await.result(queryable.get(query)).getOrElse(Seq[(K, V)]())
    // TODO: make this lazy spooling
    seq.foldRight(Spool.empty[(K, V)])((kv, spool) => ((kv)) **:: spool)
  }

  override def queryable: ReadableStore[Q, Seq[(K, V)]] = store.queryable
  
  override def getInputSplits(stores: Seq[SplittableStore[K, V, Q]], tapid: String, version: Option[Long]): Array[SplittableStoreInputSplit[K, V, Q]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, Q](tapid, sto.getWritable, version)).toArray
}