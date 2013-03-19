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

import com.twitter.storehaus.{ ReadableStore, AbstractReadableStore, Mergeable }

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
trait QueryStrategy[-Q,-L,X] extends Serializable { self =>
  // Used by the client reads
  def query(q: Q): Set[X]
  // Used in your summingbird job to flatmap your keys
  def index(key: L): Set[X]

  def cross[Q2,L2,X2](qs2: QueryStrategy[Q2,L2,X2]): QueryStrategy[(Q,Q2),(L,L2),(X,X2)] =
    new AbstractQueryStrategy[(Q,Q2),(L,L2),(X,X2)] {
      def query(q: (Q,Q2)) =
        for(x1 <- self.query(q._1); x2 <- qs2.query(q._2))
          yield (x1,x2)

      def index(key: (L,L2)) =
        for(x1 <- self.index(key._1); x2 <- qs2.index(key._2))
          yield (x1,x2)
    }
}

// For use in java/avoiding trait bloat
abstract class AbstractQueryStrategy[Q,L,X] extends QueryStrategy[Q,L,X]

object QueryStrategy extends Serializable {
  protected def multiSum[Q,K,V:Monoid](set: Set[Q], expand: (Q) => Set[K],
    resolve: (Set[K]) => Map[K,V]): Map[Q,V] = {
      // Recall which keys are needed by each query:
      val queryMap = set.map { q => (q, expand(q)) }.toMap
      // These are the unique keys to hit:
      val logicalKeys: Set[K] = queryMap.values.flatten.toSet
      // do the get and sum up the values
      val m = resolve(logicalKeys)
      queryMap.map { case (q, xs) => (q, sumValues(xs, m)) }
    }

  protected def sumValues[K,V:Monoid](ks: Set[K], m: Map[K,V]): V =
    Monoid.sum(ks.flatMap { m.get(_) })

  // TODO: This is added to algebird, use that when a new version is available
  protected def invertMap[K,V](m: Map[K,Set[V]]): Map[V,Set[K]] =
    Monoid.sum(m.toIterable.flatMap { case (k,sv) => sv.map { v => Map(v -> Set(k)) } })

  def query[Q,L,X,V:Semigroup](qs: QueryStrategy[Q,L,X], rs: ReadableStore[X,V]): ReadableStore[Q,V] =
    new AbstractReadableStore[Q,V] {
      override def get(q: Q): Future[Option[V]] = {
        val m = rs.multiGet(qs.query(q))
        // Drop the keys and sum it all up
        Monoid.sum(m.map { _._2 })
      }

      override def multiGet[Q1<:Q](qset: Set[Q1]): Map[Q1,Future[Option[V]]] =
        multiSum(qset, qs.query _, rs.multiGet[X] _)
    }

  def index[Q,L,X,V: Monoid](qs: QueryStrategy[Q,L,X], ms: Mergeable[X,V]): Mergeable[L,V] = {
    new Mergeable[L,V] {
      override def multiMerge[L1<:L](kvs: Map[L1,V]): Map[L1, Future[Unit]] = {
        // Need this to collect the futures for each L1:
        val x2lv: Map[X,(Set[L1], V)] = Monoid.sum {
          kvs.toIterable.flatMap { case (l,v) => qs.index(l).map { x => Map(x -> (Set(l),v)) } }
        }
        val xfMap: Map[X,Future[Unit]] = ms.multiMerge(x2lv.mapValues { _._2 })
        invertMap(x2lv.mapValues { _._1 }).mapValues { xs =>
          // Collect all the futures for a given l to a Future[Seq[Unit]]
          // then convert Seq[Unit] => Unit
          Future.collect(xs.map { x => xfMap(x) }.toSeq).map { _ => () }
        }
      }
    }
  }
}
