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

import com.twitter.util.Future
import com.twitter.algebird.Semigroup
/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object JMapStore {
  def apply[K,V](jmap: java.util.Map[K,V])(implicit sg: Semigroup[V]): MergeableStore[K,V] =
    new JMapStore[K,V] {
      override val jstore = jmap
      override val semigroup = sg
    }

  def empty[K,V](implicit sg: Semigroup[V]) = apply(new java.util.HashMap[K,V]())(sg)
}

abstract class JMapStore[K,V] extends MergeableStore[K,V] {
  protected val jstore: java.util.Map[K,V]

  override def get(k: K): Future[V] = Future.value(jstore.get(k))
  override def add(kv: (K,V)): Future[V] = {
    val (k,v) = kv
    val orig = jstore.get(k)
    jstore.put(k, semigroup.plus(orig, v))
    Future.value(orig)
  }
}
