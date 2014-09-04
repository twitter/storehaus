package com.twitter.storehaus.cassandra.cql

import com.twitter.util.{Future}

trait CASStore[T, K, V] {
  def get(k: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]]
  def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[(V, T)]
}
