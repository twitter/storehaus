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

import com.twitter.util.{Duration, Future, Time}
import com.twitter.finagle.redis.Client
import com.twitter.io.Buf
import com.twitter.storehaus.Store

/**
 *
 *  @author Doug Tangren
 */

object RedisSetStore {
  def apply(client: Client, ttl: Option[Duration] = RedisStore.Default.TTL): RedisSetStore =
    new RedisSetStore(client, ttl)
  def members(
      client: Client, ttl: Option[Duration] = RedisStore.Default.TTL): RedisSetMembershipStore =
    new RedisSetMembershipStore(RedisSetStore(client, ttl))
}

/**
 * A Store for sets of values backed by a Redis set.
 */
class RedisSetStore(val client: Client, ttl: Option[Duration])
  extends Store[Buf, Set[Buf]] {

  override def get(k: Buf): Future[Option[Set[Buf]]] =
    client.sMembers(k).map {
      case e if e.isEmpty => None
      case s => Some(s)
    }

  override def put(kv: (Buf, Option[Set[Buf]])): Future[Unit] =
    kv match {
      case (k, Some(v)) =>
        client.dels(Seq(k)) // put, not merge, semantics
        ttl.map(exp => client.expire(k, exp.inSeconds.toLong))
        set(k, v.toList)
      case (k, None) =>
        client.dels(Seq(k)).unit
    }

  /** Provides a view of this store for set membership */
  def members: RedisSetMembershipStore =
    new RedisSetMembershipStore(this)

  protected [redis] def set(k: Buf, v: List[Buf]) = {
    ttl.map(exp => client.expire(k, exp.inSeconds.toLong))
    client.sAdd(k, v).unit
  }

  protected [redis] def delete(k: Buf, v: List[Buf]) =
    client.sRem(k, v).unit

  override def close(t: Time): Future[Unit] = client.quit.foreach { _ => client.close() }
}

/**
 * A Store for sets of values backed by a Redis set.
 * This Store wraps a RedisSetStore providing
 * a view into set membership on a per-member basis. Set members are encoded
 * in the Store's keys as (setkey, setmember). The Store's
 * value type, Unit, simply denotes the presence of the member
 * within the Store.
 */
class RedisSetMembershipStore(store: RedisSetStore)
  extends Store[(Buf, Buf), Unit] {

  override def get(k: (Buf, Buf)): Future[Option[Unit]] =
    store.client.sIsMember(k._1, k._2).map {
      case java.lang.Boolean.TRUE => Some(())
      case _ => None
    }

  override def put(kv: ((Buf, Buf), Option[Unit])): Future[Unit] =
    kv match {
      case (key, Some(_)) => store.set(key._1, List(key._2))
      case (key, None) => store.delete(key._1, List(key._2))
    }

  override def multiPut[K1 <: (Buf, Buf)](
      kv: Map[K1, Option[Unit]]): Map[K1, Future[Unit]] = {
    // we are exploiting redis's built-in support for bulk updates and removals
    // by partioning deletions and updates into 2 maps indexed by the first
    // component of the composite key, the key of the set
    def emptyMap = Map.empty[Buf, List[K1]].withDefaultValue(Nil)
    val (del, persist) = ((emptyMap, emptyMap) /: kv) {
      case ((deleting, storing), (key, Some(_))) =>
        (deleting, storing.updated(key._1, key :: storing(key._1)))
      case ((deleting, storing), (key, None)) =>
        (deleting.updated(key._1, key :: deleting(key._1)), storing)
    }
    del.flatMap {
      case (k, members) =>
        val value = store.delete(k, members.map(_._2))
        members.map(_ -> value)
    } ++ persist.flatMap {
      case (k, members) =>
        val value = store.set(k, members.map(_._2))
        members.map(_ -> value)
    }
  }

  /** Calling close on this store will also close it's underlying
   *  RedisSetStore
   */
  override def close(t: Time): Future[Unit] = store.close(t)
}
