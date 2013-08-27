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
package com.twitter.storehaus.instrument

import com.twitter.storehaus.{ ReadableStore, Store }
import com.twitter.util.Future

/** An Instrumented type should proxy
 *  type T's public interface, capturing
 *  runtime information given the provided
 *  Instrumentation
 */
trait Instrumented[T] extends ProxyStore[T] {
  protected def instrumentation: Instrumentation
}

/** An InstrumentedReadable store
 *  captures runtime information about
 *  a ReadableStores behavior.
 */
class InstrumentedReadableStore[K, V](
  protected val self: ReadableStore[K, V],
  protected val instrumentation: Instrumentation)
  extends Instrumented[ReadableStore[K, V]]
     with ReadableStoreProxy[K,V] {

  private val getLatency =
    instrumentation.time[Future[Option[V]]]("get", "latency_ms")_
  private val getFutureLatency =
    instrumentation.timeFuture[Option[V]]("get", "future_latency_ms")_
  private val gets = instrumentation.counter("get", "total")
  private val hits = instrumentation.counter("get", "hits")
  private val misses = instrumentation.counter("get", "misses")
  private val multigets = instrumentation.counter("multi_get", "total")

  override def get(k: K): Future[Option[V]] =
    getLatency(getFutureLatency(self.get(k)).ensure {
      gets.incr()
    }.map { op =>
      if (op.isDefined) hits.incr() else misses.incr()
      op
    })

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    try self.multiGet(ks) finally {
      multigets.incr()
    }
}

class InstrumentedStore[K, V](
  protected val self: Store[K, V],
  protected val instrumentation: Instrumentation)
  extends Instrumented[Store[K, V]]
     with StoreProxy[K, V] {

  private val gets = instrumentation.counter("gets")
  private val multigets = instrumentation.counter("multi", "gets")
  private val puts = instrumentation.counter("puts")
  private val multiputs = instrumentation.counter("multi", "puts")

  override def put(kv: (K, Option[V])): Future[Unit] =
    self.put(kv).ensure {
      puts.incr()
    }
  
  override def multiPut[K1 <: K](
    kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    try self.multiPut(kvs) finally {
      multiputs.incr()
    }

  override def get(k: K): Future[Option[V]] =
    self.get(k).ensure {
      gets.incr()
    }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    try self.multiGet(ks) finally {
      multigets.incr()
    }
}
