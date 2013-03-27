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
import com.twitter.conversions.time._
import com.twitter.util.{ Future, Time }
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.{ FutureOps, MissingValueException, Store }
import org.jboss.netty.buffer.ChannelBuffer

/**
 *  @author Doug Tangren
 */

object RedisStore {
  /** A no-op ChannelBuffered type for finagle-redis's common interface */
  implicit object IdentityChannelBuffered extends ChannelBuffered[ChannelBuffer] {
    override def apply(cb: ChannelBuffer) = cb
    override def invert(cb: ChannelBuffer) = cb
  }

  object Default {
    val TTL: Option[Time] = None
  }

  def apply[T: ChannelBuffered](client: Client, ttl: Option[Time] = Default.TTL) =
    new RedisStore[T](client, ttl)
}

class RedisStore[V: ChannelBuffered](client: Client, ttl: Option[Time])
  extends Store[ChannelBuffer, V] {

  val bufferer = implicitly[ChannelBuffered[V]]

  override def get(k: ChannelBuffer): Future[Option[V]] =
    client.get(k).map(_.map(bufferer.apply(_)))

  override def multiGet[K1 <: ChannelBuffer](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val redisResult: Future[Map[ChannelBuffer, Future[Option[V]]]] = {
      // results are expected in the same order as keys
      // keys w/o mapped results are considered exceptional
      val keys = ks.toIndexedSeq.view
      client.mGet(keys).map { result =>
        val zipped = keys.zip(result).map {
          case (k, v) => (k -> Future.value(v.map(bufferer.apply(_))))
        }.toMap
        zipped ++ keys.filterNot(zipped.isDefinedAt).map { k =>
          k -> Future.exception(new MissingValueException(k))
        }
      }
    }
    FutureOps.liftValues(ks, redisResult, { (k: K1) => Future.value(Future.None) })
      .mapValues { _.flatten }
  }

  protected def set(k: ChannelBuffer, v: V) =
    ttl.map(exp => client.setEx(k, exp.inSeconds, bufferer.invert(v)))
       .getOrElse(client.set(k, bufferer.invert(v)))

  override def put(kv: (ChannelBuffer, Option[V])): Future[Unit] =
    kv match {
      case (key, Some(value)) => set(key, value)
      case (key, None) => client.del(Seq(key)).unit
    }

  override def close { client.release }
}
