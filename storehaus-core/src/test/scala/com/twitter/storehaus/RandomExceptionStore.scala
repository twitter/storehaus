package com.twitter.storehaus

import com.twitter.util.Future

import scala.util.Random

class RandomExceptionStore[K, V](f: Float = 0.5f) extends ConcurrentHashMapStore[K, V] {
  override def get(k: K): Future[Option[V]] = {
    if (Random.nextFloat() < f) Future.exception(new RuntimeException())
    else super.get(k)
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    if (Random.nextFloat() < f) Future.exception(new RuntimeException())
    else super.put(kv)
  }
}
