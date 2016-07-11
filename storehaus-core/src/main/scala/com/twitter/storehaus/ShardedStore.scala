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

package com.twitter.storehaus

import com.twitter.util.{Future, Time}

/** Factory methods to create ShardedReadableStore instances
 *  @author Oscar Boykin
 */
object ShardedReadableStore {
  def fromMap[K1, K2, V](m: Map[K1, ReadableStore[K2, V]]): ReadableStore[(K1, K2), V] = {
    val routes = new MapStore(m)
    new ShardedReadableStore[K1, K2, V, ReadableStore[K2, V]](routes) {
      override def close(time: Time) = Future.collect(m.values.map(_.close(time)).toSeq).unit
    }
  }
}

/** combines a mapping of ReadableStores into one ReadableStore that internally routes
 * Note: if K1 is absent from the routes, you will always get Future.None as a result.
 * you may want to combine this with ReadableStore.composeKeyMapping the change (K => (K1,K2))
 *
 */
class ShardedReadableStore[-K1, -K2, +V, +S <: ReadableStore[K2, V]](routes: ReadableStore[K1, S])
  extends ReadableStore[(K1, K2), V] {

  override def get(k: (K1, K2)): Future[Option[V]] = {
    val (k1, k2) = k
    FutureOps.combineFOFn[K1, S, V](routes.get, _.get(k2))(k1)
  }

  override def multiGet[T <: (K1, K2)](ks: Set[T]): Map[T, Future[Option[V]]] = {
    val prefixMap: Map[K1, Set[K2]] = ks.groupBy { _._1 }.mapValues { _.map { t => t._2 } }
    val shards: Map[K1, Future[Option[S]]] = routes.multiGet(prefixMap.keySet)
    val ksMap: Map[K1, Future[Map[K2, Future[Option[V]]]]] = shards.map { case (k1, fos) =>
      val innerm = fos.map {
        case None => Map.empty[K2, Future[Option[V]]]
        case Some(s) => s.multiGet(prefixMap(k1))
      }
      (k1, innerm)
    }
    // Now construct the result map:
    CollectionOps.zipWith(ks) { t =>
      ksMap(t._1).flatMap { m2 => m2(t._2) }
    }
  }
}

/** Factory methods to create ShardedStore instances */
object ShardedStore {
  def fromMap[K1, K2, V](m: Map[K1, Store[K2, V]]): Store[(K1, K2), V] = {
    val routes = new MapStore(m)
    new ShardedStore[K1, K2, V, Store[K2, V]](routes) {
      override def close(t: Time) = Future.collect(m.values.map(_.close(t)).toSeq).unit
    }
  }
}

/** This is only thrown when a shard is expected but somehow absent.
 * For instance, in the combinators, we expect all the underlying gets to behave correctly
 * this exception put into a Future when a user tries to put to an invalid shard
 */
class MissingShardException[K](val key: K)
    extends RuntimeException("Missing shard for prefix " + key)


/** combines a mapping of Stores into one Store that internally routes
 * Note: if a K1 is absent from the routes, any put will give a
 * {{{ Future.exception(new MissingShardException(k1)) }}}
 */
class ShardedStore[-K1, -K2, V, +S <: Store[K2, V]](routes: ReadableStore[K1, S])
    extends ShardedReadableStore[K1, K2, V, S](routes) with Store[(K1, K2), V] {
  override def put(kv: ((K1, K2), Option[V])): Future[Unit] = {
    val ((k1, k2), optv) = kv
    routes.get(k1).flatMap {
      case Some(s) => Future.value(s)
      case None => Future.exception(new MissingShardException(k1))
    }
    .flatMap { _.put((k2, optv)) }
  }
  override def multiPut[T <: (K1, K2)](kvs: Map[T, Option[V]]): Map[T, Future[Unit]] = {
    val pivoted: Map[K1, Map[K2, Option[V]]] =
      kvs.groupBy { case ((k1, _), _) => k1 } // Group by the outer key:
        // Keep just the inner key in each group
        .mapValues { maptv => maptv.map { case ((_, k2), v) => (k2, v) } }

    val shards: Map[K1, Future[Option[S]]] = routes.multiGet(pivoted.keySet)
    val shardMap: Map[K1, Future[Map[K2, Future[Unit]]]] = shards.map { case (k1, fos) =>
      val innerm = fos.map {
        case None =>
          val error = Future.exception[Unit](new MissingShardException(k1))
          pivoted(k1).mapValues { _ => error }
        case Some(s) => s.multiPut(pivoted(k1))
      }
      (k1, innerm)
    }
    // Do the lookup:
    CollectionOps.zipWith(kvs.keySet) { t =>
      shardMap(t._1).flatMap { m2 => m2(t._2) }
    }
  }
}
