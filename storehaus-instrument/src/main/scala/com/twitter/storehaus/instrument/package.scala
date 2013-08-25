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

/** Package instrument defines interfaces that proxy and collect runtime information about Stores.
 *
 *  = instrumentation =
 *
 *  A type Instrumentation is defined which implements a set of interfaces for recording information
 *  about a store. This set of interfaces allows instrumented stores to collect Counters, Stats, and Gauges.
 *  An instrumented store will typically ask the instrumentation for a reference to one of these primtives
 *  and then apply collected information at runtime.
 *
 *  {{{
 *  import com.twitter.storehaus.instrument.Instrumentation
 *  object MemoryInstrumentation implements Instrumentation {
 *    @volatile private[this] var counters = Map.empty[Seq[String], Counter]
 *    def counter(name: String*) = {
 *      if (!counters.contains(name)) synchronized {
 *        val counter = new Counter {
 *          private[this] val underlying = java.util.concurrent.atomic.AtomicInteger
 *          def incr(delta: Int = 1) { underlying.addAndAdd(delta) }
 *        }
 *        counters += (name -> counter)
 *      }
 *      counters(name)
 *    }
 *  }
 *  }}}
 *
 *  = instrumenting =
 *
 *  An Instrumented type is defined as a proxy for a store that will write information to a provided Instrumentation
 *  instance over the course of its lifespan.
 *
 *  In order to ensure Instrumented types expose their subjects behavior, Instrumented stores also implement a ProxyStore.
 *  A StoreProxy is an interface defined for each store type that implements the types interfaces
 *  and simply forwards method calls to the instance being proxied.
 *
 *  {{{
 *  trait ProxyStore[T] {
 *    def self: T
 *  }
 *  trait ReadableStoreProxy[K, V] extends ProxyStore[ReadableStore[K, V]] with ReadableStore[K, V] {
 *    def get(k: K): Future[Option[V]] = self.get(k)
 *    ...
 *  }
 *  }}} 
 *
 *  Instrumented stores follow the following type
 * 
 *  {{{
 *  trait Instumented[T] extends StoreProxy[T] {
 *    def instrumentation: Instrumentation
 *  }
 *  }}}
 *
 *  {{{
 *  class InstrumentedReadableStore[K,V](
 *    val self: ReadableStore[K, V],
 *    val instrumentation: Instrumentation)
 *    extends Instrumented[ReadableStore[K, V]] with ReadableStoreProxy[K,V] {
 *    val gets = instrumentations.counter("gets")
 *    override def get(k: K): Future[Option[V]] = {
 *      store.get(k).ensure {
 *         gets.incr()
 *      }
 *    }
 *  }
 *  }}}
 * 
 *  = enrichment =
 *
 *  The act of instrumenting a Store is exposed through the process of enrichment
 * 
 *  {{{
 *  import com.twitter.storehaus.ReadableStore
 *  import com.twitter.storehaus.instrument
 *  val store = ReadableStore.fromFn(_.toString).instrument()
 *  }}}
 *
 *  This will have the following effect.
 *  1) A ReadableStore is created from a provided function
 *  2) An implicit conversion is resolved for the type of store, in this case a ReadableStore,
 *     to a class instance of Instrumented which wraps the store, and proxies all of the store's
 *     methods, and is able collect runtime information.
 *
 *  {{{
 *  // enrichment type to provide the `instrument` method on a given store.
 *  implicit class InstrumentingReadableStore(store: ReadableStore[K, V]) {
 *    def instrument(instrumentation: Instrumentation = DefaultInstrumentation) =
 *      new InstrumentedReadableStore(store, instrumented)
 *  }
 *  }}}
 *
 * 3) A proxy interface implementing instrumentation of the underlying store's methods is returned
 *
 * Lastly, if you have more than one store of a given type, you may choose to scope intrumentation results
 * based on a usage label. You can do this by calling the `prefix(name)` method on the store returned by instrumented store.
 *
 * {{{
 *  val store = ReadableStore.fromFn(_.toString).instrument(DevInstrumentation).prefix("app")
 * }}}
 *
 * This will effectively prefix the exported instrumentation labels with the given string. Alternatively
 * you may apply a suffix with `suffix(name)` or both.
 *
 * {{{
 * val store = ReadableStore.fromFn(_.toString).instrument(DevInstrumentation).prefix("app").suffix("hostname")
 * }}}
 *
 */
package object instrument
