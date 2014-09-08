package com.twitter.storehaus.cassandra.cql

import com.twitter.util.{Future, Return, Throw}

/**
 * represents store supporting Compare/Check and Set. This allows to implement 
 * distributed asynchronous "transaction-like" operations. E.g. a distributed 
 * lock implemented with this and Futures is a snap.
 */
trait CASStore[T, K, V] {
  
  def get(k: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]]
  
  def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[Boolean]  
  
  /**
   * execute cas and get sequence. Be aware that the get is not
   * executed atomically together with the cas
   */
  def casAndGet(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[(Boolean, Option[(V, T)])] = 
    cas(token, kv).flatMap(success => get(kv._1).flatMap(vt => Future((success, vt))))
}
