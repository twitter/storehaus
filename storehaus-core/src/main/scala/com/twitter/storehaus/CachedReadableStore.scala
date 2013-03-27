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

import com.twitter.storehaus.cache.Cache
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

// TODO: Should we throw some special exception about a value that
// never made it into the cache vs Future.None?

class CachedReadableStore[K, V](store: ReadableStore[K, V], cache: Cache[K, Future[Option[V]]]) extends ReadableStore[K, V] {
  val cacheRef = Atomic[Cache[K, Future[Option[V]]]](cache)

  def swapCache(cache: Cache[K, Future[Option[V]]]) {
    cacheRef.update { _ => cache }
  }

  override def get(k: K): Future[Option[V]] =
    cacheRef.update { _.touch(k)(store.get(_)) }
      .get(k).getOrElse(Future.None)

  override def multiGet[K1 <: K](keys: Set[K1]): Map[K1, Future[Option[V]]] = {
    val touched = cacheRef.update { _.multiTouch(keys.toSet[K])(store.multiGet(_)) }
    CollectionOps.zipWith(keys) { touched.get(_).getOrElse(Future.None) }
  }
}

// Thanks to http://blog.scala4java.com/2012/03/atomic-update-of-atomicreference.html
object Atomic {
  def apply[T](obj: T) = new Atomic(obj)
}

class Atomic[T](obj: T) {
  protected val atomic: AtomicReference[T] = new AtomicReference(obj)

  /**
  * Update and return the new state
  * NO GUARANTEE THAT update IS ONLY CALLED ONCE!!!!
  * update should return a new state based on the old state
  */

  @tailrec
  final def update(f: T => T): T = {
    val oldValue = atomic.get()
    val newValue = f(oldValue)
    if (atomic.compareAndSet(oldValue, newValue)) newValue else update(f)
  }

  def get() = atomic.get()
}
