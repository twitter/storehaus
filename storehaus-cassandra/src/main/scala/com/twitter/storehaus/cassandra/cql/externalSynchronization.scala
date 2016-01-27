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

import scala.collection.mutable.HashMap

import com.twitter.storehaus.WritableStore
import com.twitter.concurrent.AsyncMutex
import com.twitter.concurrent.Permit
import com.twitter.util.{Await, Duration, Future, FuturePool, Try}
import com.twitter.zk.ZkClient
import com.twitter.zk.coordination.ZkAsyncSemaphore

import java.util.concurrent.TimeUnit

/**
 * setup two types of synchronization for the
 * CQLCassandraMergeableStore individually
 */
case class CassandraExternalSync (
  val put : ExternalSynchronization,
  val merge : ExternalSynchronization
)


/**
 * for some use cases external synchronization is required
 */
sealed trait ExternalSynchronization {
  def lock[T](id: String, f: => T): Future[T]
}

case class NoSync() extends ExternalSynchronization {
  override def lock[T](id: String, f: => T) = Future.value(f)
}

/**
 * sync based on util-zk
 * Example in the docs of ZkAsyncSemaphore
 */
case class ZkSync(val zkclient: ZkClient, val maxWaiters: Int) extends ExternalSynchronization {
  override def lock[T](id: String, f: => T): Future[T] = {
    val semaphore = new ZkAsyncSemaphore(zkclient, id, maxWaiters)
    semaphore.acquire().map { permit => Try(f).ensure(permit.release).get }
  }
} 

/**
 * Only one client VM possible with local sync
 * Different stores are supported using  <storeId>'->'<id> 
 */
case class LocalSync(val storeId: String, val maxWaiters: Int) extends ExternalSynchronization {
  LocalSync.init()
  override def lock[T](id: String, f: => T): Future[T] = {
    LocalSync.getLock(storeId, id, maxWaiters).acquire().map { permit => Try(f).ensure(permit.release).get }
  }
}
 
object LocalSync {
  var lockMap: Option[HashMap[String, AsyncMutex]] = None
  def init() {
    lockMap.synchronized({
      lockMap match {
        case None => lockMap = Some(new HashMap[String, AsyncMutex])
        case _ => 
      }
    })
  }
  def getLock(storeId: String, id: String, maxWaiters: Int): AsyncMutex = {
    val mId = storeId + "->" + id
    lockMap.synchronized({
      lockMap.get.get(mId).getOrElse {
        val mutex = new AsyncMutex(maxWaiters)
        lockMap.get += mId -> mutex
        mutex
      } 
    })
  }
}

/**
 * sync based on Cassandra's compare-and-set
 */
case class CassandraCASSync(val store: CASStore[Long, String, Boolean] with WritableStore[String, Option[Boolean]], 
                            val busyWaitLoopTime: Duration, val futurePool: FuturePool) extends ExternalSynchronization {
  override def lock[T](id: String, f: => T): Future[T] = futurePool {
    // poll store until we know it's finished
    val token = TokenFactory.longTokenFactory.createNewToken
    def busyWaitForAquiredLock: T = {
      def busyWaitForReleasedLock(): Unit = {
        val lockCol = Await.result(store.get(id))
        if(lockCol != None && lockCol.get._1) {
          Thread.sleep(busyWaitLoopTime.inUnit(TimeUnit.MILLISECONDS))
          busyWaitForReleasedLock
        }
      }
      if(Await.result(store.cas(Some(token), (id, true)))) {
        Try(f).ensure { 
          store.put((id, None))
        }.get
      } else {
        busyWaitForAquiredLock
      }
    }
    busyWaitForAquiredLock
  }
}

object mapKeyToSyncId {
  def apply(key: AnyRef, family: CQLCassandraConfiguration.StoreColumnFamily) = s"""\"${family.getName}\".""" + key.toString()  
}
