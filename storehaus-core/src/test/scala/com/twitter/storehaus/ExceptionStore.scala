package com.twitter.storehaus

import com.twitter.util.Future

class ExceptionStore[K, V](f: Float = 0.5f) extends Store[K, V] {
  override def get(k: K): Future[Option[V]] = {
    Future.exception(new RuntimeException())
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    Future.exception(new RuntimeException())
  }
}
