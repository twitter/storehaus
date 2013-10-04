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

package com.twitter.storehaus.redis

import com.twitter.algebird.Monoid
import com.twitter.util.Future
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.Store
import com.twitter.storehaus.algebra.MergeableStore
import org.jboss.netty.buffer.ChannelBuffer

object RedisSortedSetStore {
  def apply(client: Client) =
    new RedisSortedSetStore(client)
}

/** A Store representation of a redis sorted set
 *  where keys represent the name of the set and values
 *  represent both the member's name and score within the set
 */
class RedisSortedSetStore(client: Client)
  extends MergeableStore[ChannelBuffer, Seq[(ChannelBuffer, Double)]] {
  val monoid = implicitly[Monoid[Seq[(ChannelBuffer, Double)]]]

  /** Returns the whole set as a tuple of seq of (member, score).
   *  An empty set is represented as None. */
  override def get(k: ChannelBuffer): Future[Option[Seq[(ChannelBuffer, Double)]]] =
    client.zRange(k, 0, -1, true).map(
      _.left.toOption.map( _.asTuples).filter(_.nonEmpty)
    )
    
  /** Replaces or deletes the whole set. Setting the set effectivly results
   *  in a delete of the previous sets key and multiple calls to zAdd for each member. */
  override def put(kv: (ChannelBuffer, Option[Seq[(ChannelBuffer, Double)]])): Future[Unit] =
    kv match {
      case (set, Some(scorings)) =>
        client.del(Seq(set)).flatMap { _ =>
          Future.collect(members.multiPut(scorings.map {
            case (member, score) => ((set, member), Some(score))
          }.toMap).values.toSeq).unit
        }
      case (set, None) =>
        client.del(Seq(set)).unit
    }

  /** Performs a zIncrBy operation on a set for a seq of members */
  override def merge(kv: (ChannelBuffer, Seq[(ChannelBuffer, Double)])): Future[Unit] =
    Future.collect(kv._2.map {
      case (member, by) => client.zIncrBy(kv._1, by, member)
    }).unit

  /** @return a mergeable store backed by redis with this store's client */
  def members: MergeableStore[(ChannelBuffer, ChannelBuffer), Double] =
    new RedisSortedSetMembershipStore(client)

  /** @return a mergeable store for a given set with this store's client */
  def members(set: ChannelBuffer): MergeableStore[ChannelBuffer, Double] =
    new RedisSortedSetMembershipView(client, set)

  override def close = client.release
}

/** An unpivoted-like member-oriented view of a redis sorted set bound to a specific
 *  set. Keys represent members. Values represent the members score
 *  within the given set. Work is delegated to an underlying
 *  RedisSortedSetMembershipStore. For multiPuts containing deletes, it is more
 *  efficient to use a RedisSortedSetMembershipStore directly.
 *
 *  These stores also have mergeable semantics via zIncrBy for a member's
 *  score.
 */
class RedisSortedSetMembershipView(client: Client, set: ChannelBuffer)
  extends MergeableStore[ChannelBuffer, Double] {
  private lazy val underlying = new RedisSortedSetMembershipStore(client)
  val monoid = implicitly[Monoid[Double]]

  override def get(k: ChannelBuffer): Future[Option[Double]] =
    underlying.get((set, k))

  override def put(kv: (ChannelBuffer, Option[Double])): Future[Unit] =
    underlying.put(((set,kv._1), kv._2))

  override def merge(kv: (ChannelBuffer, Double)): Future[Unit] =
    underlying.merge((set, kv._1), kv._2)

  override def close = underlying.close
}

/** An unpivoted-like member-oriented view of redis sorted sets.
 *  Keys represent the both a name of the set and the member.
 *  Values represent the member's current score within a set.
 *  An absent score also indicates an absence of membership in the set.
 *
 *  These stores also have mergeable semantics via zIncrBy for a member's
 *  score
 */
class RedisSortedSetMembershipStore(client: Client)
  extends MergeableStore[(ChannelBuffer, ChannelBuffer), Double] {
  val monoid = implicitly[Monoid[Double]]

  /** @return a member's score or None if the member is not in the set */
  override def get(k: (ChannelBuffer, ChannelBuffer)): Future[Option[Double]] =
    client.zScore(k._1, k._2).map(_.map(_.toDouble))
   
   /** Partitions a map of multiPut pivoted values into
    *  a two item tuple of deletes and sets, multimapped
    *  by a key computed from K1.
    *
    *  This makes partioning deletes and sets for pivoted multiPuts
    *  easier for stores that can perform batch operations on collections
    *  of InnerK values keyed by OutterK where V indicates membership
    *  of InnerK within OutterK.
    * 
    *  ( general enough to go into PivotOpts )
    */
   def multiPutPartitioned[OutterK, InnerK, K1 <: (OutterK, InnerK), V, IndexK](kv: Map[K1, Option[V]])(by: K1 => IndexK):
    (Map[IndexK, List[(K1, Option[V])]], Map[IndexK, List[(K1, Option[V])]]) = {
      def emptyMap = Map.empty[IndexK, List[(K1, Option[V])]].withDefaultValue(Nil)
      ((emptyMap, emptyMap) /: kv) {
        case ((deleting, storing), (key, value @ Some(_))) =>
          val index = by(key)
          (deleting, storing.updated(index, (key, value) :: storing(index)))
        case ((deleting, storing), (key, _)) =>
          val index = by(key)
          (deleting.updated(index, (key, None) :: deleting(index)), storing)
      }
    }

  /** Adds or removes members from sets with an initial scoring. A score of None indicates the
   *  member should be removed from the set */
  override def multiPut[K1 <: (ChannelBuffer, ChannelBuffer)](kv: Map[K1, Option[Double]]): Map[K1, Future[Unit]]  = {
    // we are exploiting redis's built-in support for removals (zRem)
    // by partioning deletions and updates into 2 maps indexed by the first
    // component of the composite key, the key of the set
    val (del, persist) = multiPutPartitioned[ChannelBuffer, ChannelBuffer, K1, Double, ChannelBuffer](kv)(_._1)
    (del.map {
      case (k, members) =>
        val value = client.zRem(k, members.map(_._1._2))
      members.map(_._1 -> value.unit)
    }.flatten ++ persist.map {
      case (k, members) =>
        members.map {
          case (k1, score) =>
            // a per-InnerK operation
            (k1 -> client.zAdd(k, score.get, k1._2).unit)
        }
    }.flatten).toMap
  }

  /** Performs a zIncrBy operation on a set for a given member */
  override def merge(kv: ((ChannelBuffer, ChannelBuffer), Double)): Future[Unit] =
    client.zIncrBy(kv._1._1, kv._2, kv._1._2).unit

  override def close = client.release
}
