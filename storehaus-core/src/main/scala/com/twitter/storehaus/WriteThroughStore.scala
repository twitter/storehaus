/*
 * Copyright 2014 Twitter Inc.
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

import com.twitter.util.Future

/**
 * Provides write-through caching on a store fronted by a cache.
 *
 * @author Ruban Monu
 */
class WriteThroughStore[K, V](backingStore: Store[K, V], cache: Store[K, V], invalidate: Boolean = true)
  extends ReadThroughStore[K, V](backingStore, cache) with Store[K, V] {

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    // should we instead try and write to cache store first?
    backingStore.multiPut(kvs).map { kv: (K1, Future[Unit]) =>
      val f: Future[Unit] = kv._2.flatMap { u: Unit =>
          cache.put((kv._1, kvs.getOrElse(kv._1, None)))
        } handle { case x: Exception =>
          if (invalidate) cache.put((kv._1, None)).flatMap { u: Unit => throw x }
          else throw x
        }
      (kv._1, f)
    }
    // TODO: make the cache writes batched as well
  }
}

