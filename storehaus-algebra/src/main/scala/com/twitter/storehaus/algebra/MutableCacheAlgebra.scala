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

package com.twitter.storehaus.algebra

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.storehaus.cache._

/**
  * Enrichments and modifications to instances of MutableCache. Use
  * these enrichments by importing `MutableCacheAlgebra._`.
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  */

object MutableCacheAlgebra {
  implicit def enrich[K, V](cache: MutableCache[K, V]): AlgebraicMutableCache[K, V] =
    new AlgebraicMutableCache[K, V](cache)

  def mutableFromTTL[K, V](ttlCache: TTLCache[K, V]): MutableCache[K, V] =
    ttlCache.toMutable()
      .inject(new TTLInjection[K, Long, V](ttlCache.ttl.inMilliseconds)(ttlCache.clock))
}

class AlgebraicMutableCache[K, V](cache: MutableCache[K, V]) {
  def inject[U](injection: Injection[(K, U), (K, V)]): MutableCache[K, U] =
    new MutableCache[K, U] {
      override def get(k: K) = for {
        v <- cache.get(k)
        (_, u) <- injection.invert(k, v).toOption
      } yield u

      override def +=(ku: (K, U)) = { cache += injection(ku); this }

      override def hit(k: K) =
        cache.hit(k)
          .flatMap(injection.invert(k, _).toOption).map(_._2)

      override def evict(k: K) = for {
        evictedV <- cache.evict(k)
        (_, evictedU) <- injection.invert(k, evictedV).toOption
      } yield evictedU

      override def empty = new AlgebraicMutableCache(cache.empty).inject(injection)

      override def clear = { cache.clear; this }
      override def iterator =
        cache.iterator.flatMap(injection.invert(_).toOption)
    }
}
