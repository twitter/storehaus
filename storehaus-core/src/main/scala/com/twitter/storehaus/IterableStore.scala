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

/**
 * Trait for iterating over the key space in a ReadableStore.
 * Depending on the backing store, this may have performance implications. So use with caution.
 *
 * In general, this should be okay to use with cache stores.
 * For other stores, the iterable should ideally be backed by a stream.
 *
 * @author Ruban Monu
 */
trait IterableReadableStore[-K, V] extends ReadableStore[K, V] {

  def getAll[K1 <: K]: Future[Iterable[(K1, V)]]
  // TODO: have a version that returns just the keyset?

  def getAllWithFilter[K1 <: K](f: K1 => Boolean): Future[Iterable[(K1, V)]] =
    getAll.map { kvIter: Iterable[(K1, V)] => kvIter.filter { case kv => f(kv._1) } }
}

trait IterableStore[-K, V] extends IterableReadableStore[K, V] with Store[K, V]

