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
import com.twitter.storehaus.{Store, UnpivotedStore}

/**
 *
 *  @author Doug Tangren
 */

object RedisHashStore {

  def apply(client: Client, ttl: Option[Duration] = RedisStore.Default.TTL): RedisHashStore =
    new RedisHashStore(client, ttl)

  def unpivoted(
      client: Client, ttl: Option[Duration] = RedisStore.Default.TTL): UnpivotedRedisHashStore =
    new UnpivotedRedisHashStore(apply(client, ttl))
}

/**
 * A Store in which keys map to Maps of secondary keys and values backed
 * by a redis hash
 */
class RedisHashStore(val client: Client, ttl: Option[Duration])
  extends Store[Buf, Map[Buf, Buf]] {

  override def get(k: Buf): Future[Option[Map[Buf, Buf]]] =
    client.hGetAll(k).map {
      case e if e.isEmpty => None
      case xs => Some(Map(xs: _*))
    }

  protected def set(k: Buf, v: Map[Buf, Buf]) = {
    ttl.map(exp => client.expire(k, exp.inSeconds.toLong))
    client.hMSet(k, v).unit
  }

  override def put(kv: (Buf, Option[Map[Buf, Buf]])): Future[Unit] =
    kv match {
      case (key, Some(value)) => set(key, value)
      case (key, None) => client.dels(Seq(key)).unit
    }

  override def close(t: Time): Future[Unit] = client.quit.foreach { _ => client.close() }
}

/*
 *  A Store in which K is a tuple of (Key, FieldKey) and V is
 *  the value of associated with FieldKey within the redis hash
 */
class UnpivotedRedisHashStore(hstore: RedisHashStore)
  extends UnpivotedStore[(Buf, Buf),
    Buf, Buf, Buf](hstore)(identity)
