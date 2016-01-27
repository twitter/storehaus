/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.storehaus.cassandra.cql

import com.twitter.util.Future

/**
 * represents store supporting Compare/Check and Set. This allows to implement 
 * distributed asynchronous "transaction-like" operations. E.g. a distributed 
 * lock implemented with this and Futures is a snap.
 */
trait CASStore[T, K, V] {
  
  def get(k: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]]
  
  /**
   * executes a check-and-set operation as a transaction (sort of)
   * If token is None cas changes external only if there is no previous value (and returns true).
   * If it is Some(t), then the cas only changes state and returns true iff the encountered token in
   *  the store is the same as t.
   */
  def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[Boolean]  
  
  /**
   * execute cas and get sequence. Be aware that the get is not
   * executed atomically together with the cas.
   */
  def casAndGet(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[(Boolean, Option[(V, T)])] = 
    cas(token, kv).flatMap(success => get(kv._1).flatMap(vt => Future((success, vt))))
}
