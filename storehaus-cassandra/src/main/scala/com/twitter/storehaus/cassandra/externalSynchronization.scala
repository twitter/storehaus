package com.twitter.storehaus.cassandra

import com.twitter.util.Future
import com.twitter.zk.coordination.ZkAsyncSemaphore
import com.twitter.zk.ZkClient
import com.twitter.concurrent.Permit

/**
 * setup two types of synchronization for the
 * CassandraLongStore individually
 */
case class CassandraLongSync (
  val put : ExternalSynchronization,
  val merge : ExternalSynchronization
)


/**
 * for some cases external synchronization is required to provide
 * a higher level of consistency
 */
sealed trait ExternalSynchronization {
  def lock[T](id: String, future: Future[T]): Future[T]
}

/**
 * this lock doesn't care for concurrency
 */
case class NoSync() extends ExternalSynchronization {
  override def lock[T](id: String, future: Future[T]) = future
}

/**
 * sync based on util-zk
 * Example in the docs ZkAsyncSemaphore
 */
case class ZkSync(val zkclient: ZkClient) extends ExternalSynchronization {
  override def lock[T](id: String, future: Future[T]) = {
    val semaphore = new ZkAsyncSemaphore(zkclient, id, 1)
    semaphore.acquire().flatMap { permit => future.ensure(permit.release)}
  }
} 



