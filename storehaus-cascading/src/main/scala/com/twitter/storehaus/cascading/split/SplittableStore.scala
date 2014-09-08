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
package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.ReadableStore
import com.twitter.concurrent.Spool
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputSplit

/**
 * Idea: split store into multiple sub-stores which are essentially splits on the store.
 * 
 * i.e. simplification of c.f. https://github.com/twitter/storehaus/pull/191
 */
trait SplittableStore[K, V, Q <: Writable, T <: SplittableStore[K, V, Q, T]] {

  /**
   * return a set of splits from this SplittableStore
   * the Int param is just a hint similar to InputFormat from Hadoop
   */
  def getSplits(numberOfSplitsHint: Int): Seq[T]

  /**
   * get a single split (Store) from this representation of a set of keys
   */
  def getSplit(predicate: Q): T
  
  /**
   * enumerates keys in this SplittableStore
   */
  def getAll: Spool[(K, V)]
  
  /**
   * converts SplittableStores into their InputSplit counterparts
   */
  def getInputSplits(stores: Seq[T], tapid: String): Array[SplittableStoreInputSplit[K, V, Q]]
}