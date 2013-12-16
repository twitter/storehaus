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

package com.twitter.storehaus.algebra.query

import com.twitter.storehaus.{ AbstractReadableStore, FutureOps, ReadableStore }
import com.twitter.storehaus.algebra.MergeableStore

import com.twitter.algebird.{Semigroup, Monoid}
import com.twitter.algebird.MapAlgebra
import com.twitter.algebird.util.UtilAlgebras._
import com.twitter.util.Future

import java.io.Serializable

/** Query support for summingbird beyond Key-Value
 * Q represents the query object
 * L represents the logical input keys from your data
 * X represents the expansion of logical keys (can't co variant because Set is not)
 */
trait QueryStrategy[-Q, -L, X] extends Serializable { self =>
  /** Used by the client reads, read all these X and sum the result */
  def query(q: Q): Set[X]
  /** Used in your summingbird job to flatmap your keys, increment all these X with the value */
  def index(key: L): Set[X]

  /** Create new strategy on a this and a second query strategy */
  def cross[Q2, L2, X2](qs2: QueryStrategy[Q2, L2, X2]): QueryStrategy[(Q, Q2), (L, L2), (X, X2)] =
    new AbstractQueryStrategy[(Q, Q2), (L,L2), (X,X2)] {
      def query(q: (Q, Q2)) =
        for(x1 <- self.query(q._1); x2 <- qs2.query(q._2))
          yield (x1,x2)

      def index(key: (L, L2)) =
        for(x1 <- self.index(key._1); x2 <- qs2.index(key._2))
          yield (x1,x2)
    }
}

/** For use in java/avoiding trait bloat. Avoid using in APIs */
abstract class AbstractQueryStrategy[Q, L, X] extends QueryStrategy[Q, L, X]

/** Factory methods and combinators on QueryStrategies */
object QueryStrategy extends Serializable {
  protected def multiSum[Q, K, V:Monoid](set: Set[Q], expand: (Q) => Set[K],
    resolve: (Set[K]) => Map[K, V]): Map[Q,V] = {
      // Recall which keys are needed by each query:
      val queryMap = set.map { q => (q, expand(q)) }.toMap
      // These are the unique keys to hit:
      val logicalKeys: Set[K] = queryMap.values.flatten.toSet
      // do the get and sum up the values
      val m = resolve(logicalKeys)
      queryMap.map { case (q, xs) => (q, sumValues(xs, m)) }
    }

  protected def sumValues[K, V:Monoid](ks: Set[K], m: Map[K,V]): V =
    Monoid.sum(ks.flatMap { m.get(_) })

  /** Given a QueryStrategry and a ReadableStore which has been indexed correctly, give a ReadableStore on Queries
   */
  def query[Q,L,X,V:Semigroup](qs: QueryStrategy[Q, L, X], rs: ReadableStore[X, V]): ReadableStore[Q, V] =
    new AbstractReadableStore[Q, V] {
      override def get(q: Q): Future[Option[V]] = {
        val m = rs.multiGet(qs.query(q))
        // Drop the keys and sum it all up
        Monoid.sum(m.map { _._2 })
      }

      override def multiGet[Q1 <: Q](qset: Set[Q1]): Map[Q1, Future[Option[V]]] =
        multiSum(qset, qs.query _, rs.multiGet[X] _)
    }

  // TODO: Think about whether we need to return some sort of Sink
  // type vs a Function1.
  /** Given a query strategry and a MergeableStore, return a function that accepts pairs of (L,V)
   * and merges them into the store so they can be queried with this strategy.
   */
  def index[Q, L, X, V](qs: QueryStrategy[Q, L, X], ms: MergeableStore[X, V]): (TraversableOnce[(L, V)] => Future[Unit]) = { ts =>
    implicit val sg: Semigroup[V] = ms.semigroup
    val summed: Map[X, V] = Monoid.sum {
      ts.flatMap { case (l,v) => qs.index(l).map { x => Map(x -> v) } }
    }
    FutureOps.mapCollect(ms.multiMerge(summed)).unit
  }
}
