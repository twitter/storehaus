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
package com.twitter.storehaus.cascading.split

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.util.Await
import com.twitter.storehaus.cascading.{ InitializableStoreObjectSerializer, Instance }
import org.apache.hadoop.io.Writable
import scala.collection.JavaConversions._
import java.net.InetAddress
import org.slf4j.{ Logger, LoggerFactory }

/**
 * SplittableStore based implementation of a SplittingMechanism
 */
class SplittableStoreSplittingMechanism[K, V, Q <: Writable, T <: SplittableStore[K, V, Q], U <: AbstractSplittableStoreCascadingInitializer[K, V, Q, T]](override val conf: JobConf) 
	extends StorehausSplittingMechanism[K, V, U](conf: JobConf) {
  @transient private val log = LoggerFactory.getLogger(classOf[SplittableStoreSplittingMechanism[K, V, Q, T, U]])
  
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val version = InitializableStoreObjectSerializer.getReadVerion(conf, tapid)
  val storeinit = version match {
    case None => Left(InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[SplittableStoreCascadingInitializer[K, V, Q, T]])
    case Some(version) => Right(InitializableStoreObjectSerializer.getReadableVersionedStoreIntializer(conf, tapid, version).get.asInstanceOf[VersionedSplittableStoreCascadingInitializer[K, V, Q, T]])
  }
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    log.debug(s"Getting splits for StorehausTap with id $tapid from SplittableStore")
    val splittableStore = storeinit match {
      case Left(initsimple) => initsimple.getSplittableStore(job).get
      case Right(initversioned) => initversioned.getSplittableStore(job, version.get).get
    }
    val splittableStores = splittableStore.getSplits(hint)
    splittableStore.getInputSplits(splittableStores, tapid, version).asInstanceOf[Array[InputSplit]]
  }
  
  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    log.debug(s"Initializing Spool for StorehausTap with id $tapid")
    val versionFromSplit = getStorehausSplit(split).version
    getStorehausSplit(split).spool = Some((storeinit match {
      case Left(initsimple) => initsimple.getSplittableStore(conf).get
      case Right(initversioned) => initversioned.getSplittableStore(conf, versionFromSplit.get).get
    }).getSplit(getStorehausSplit(split).getPredicate, versionFromSplit).getAll)
  }
  
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    val storesplit = getStorehausSplit(split)
    if(!(storesplit.spool.isEmpty || storesplit.spool.get.isEmpty)) {
      val (keyObj, valueObj) = storesplit.spool.get.head
      log.debug(s"Filling record for StorehausTap with id $tapid with value=$valueObj and key=$keyObj into store " + storeinit) 
      key.set(keyObj)
      value.set(valueObj)
      storesplit.spool = Some(Await.result(storesplit.spool.get.tail))
      true
    } else false
  }
  
  private def getStorehausSplit(split: InputSplit): SplittableStoreInputSplit[K, V, Q] = split.asInstanceOf[SplittableStoreInputSplit[K, V, Q]]
  
  override def close = {}
}
