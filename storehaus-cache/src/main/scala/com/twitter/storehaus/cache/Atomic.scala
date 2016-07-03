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

package com.twitter.storehaus.cache

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

// Thanks to http://blog.scala4java.com/2012/03/atomic-update-of-atomicreference.html

object Atomic {
  def apply[T <: AnyRef](obj: T): Atomic[T] = new Atomic(obj)
}

class Atomic[T <: AnyRef](obj: T) {
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

  @tailrec
  final def effect[S](f: T => (S, T)): (S, T) = {
    val oldValue = atomic.get()
    val withEffect = f(oldValue)
    if (atomic.compareAndSet(oldValue, withEffect._2)) withEffect else effect(f)
  }

  def get(): T = atomic.get()
}
