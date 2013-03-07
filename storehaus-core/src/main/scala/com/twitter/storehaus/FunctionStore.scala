/*
 * Copyright 2010 Twitter Inc.
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
 *  @author Oscar Boykin
 */

/** Creates ReadableStores based on functions.
 * Purpose is to use functions with ReadableStore combinators
 */
object FunctionStore {
  /** Treat a Function1 like a ReadableStore
   */
  def apply[K,V](getfn: (K) => Option[V]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = // I know Future(getfn(k)) looks similar, we've seen some high costs with that
      try { Future.value(getfn(k)) }
      catch { case e: Throwable => Future.exception(e) }
  }

  /** Treat a PartialFunction like a ReadableStore
   */
  def partial[K,V](getfn: PartialFunction[K,V]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = if(getfn.isDefinedAt(k)) {
      try { Future.value(Some(getfn(k))) }
      catch { case e: Throwable => Future.exception(e) }
    } else Future.None
  }
  /** Treat a function returning a Future[Option[V]] as the get method of a ReadableStore
   */
  def future[K,V](getfn: (K) => Future[Option[V]]): ReadableStore[K,V] = new AbstractReadableStore[K,V] {
    override def get(k: K) = getfn(k)
  }
}
