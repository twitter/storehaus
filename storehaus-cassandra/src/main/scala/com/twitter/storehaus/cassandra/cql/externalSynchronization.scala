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
package com.twitter.storehaus.cassandra.cql

import scala.collection.mutable.HashMap

import com.twitter.concurrent.AsyncMutex
import com.twitter.concurrent.Permit
import com.twitter.util.Future
import com.twitter.zk.ZkClient
import com.twitter.zk.coordination.ZkAsyncSemaphore

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
  def lock[T](id: String, future: Future[T]): Future[T]
}

case class NoSync() extends ExternalSynchronization {
  override def lock[T](id: String, future: Future[T]) = future
}

/**
 * sync based on util-zk
 * Example in the docs of ZkAsyncSemaphore
 */
case class ZkSync(val zkclient: ZkClient) extends ExternalSynchronization {
  override def lock[T](id: String, future: Future[T]) = {
    val semaphore = new ZkAsyncSemaphore(zkclient, id, 1)
    semaphore.acquire().flatMap { permit => future.ensure(permit.release)}
  }
} 

/**
 * Only one client VM possible with local sync
 * Different stores are supported using  <storeId>'->'<id> 
 */
case class LocalSync(val storeId: String, val maxWaiters: Int) extends ExternalSynchronization {
  LocalSync.init()
  override def lock[T](id: String, future: Future[T]) = {  
    LocalSync.getLock(storeId, id, maxWaiters).acquire().flatMap { permit => future.ensure(permit.release)}
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
        lockMap.get += mId -> new AsyncMutex(maxWaiters)
        lockMap.get.get(mId).get
      } 
    })
  }
}

object mapKeyToSyncId {
  def apply(key: AnyRef, family: CQLCassandraConfiguration.StoreColumnFamily) = s"""\"${family.getName}\".""" + key.toString()  
}
