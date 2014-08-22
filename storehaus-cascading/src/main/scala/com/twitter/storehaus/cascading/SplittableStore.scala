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
package com.twitter.storehaus.cascading

import com.twitter.storehaus.ReadableStore

/**
 * c.f. https://github.com/twitter/storehaus/pull/191
 * 
 * used for implementing Hadoop InputSplits and use Storehaus as a cascading tap.
 */
class SplittableStore[K, V](store: SubsettableStore[K, V]) extends ReadableStore[K, V] {

  def getSplits(numberOfSplitsHint: Int): Iterable[SubsettableStore[K, V]] = {
    store match {
      case pistore: PartiallyIterableStore[K, V] => null
      case custore: CursoredStore[K, V] => null
      case ofstore: OffsettableStore[K, V] => null
      case _ => throw new RuntimeException("SplittableStore: provided store must be an implementation of SubsettableStore");
    }
  }
  
}