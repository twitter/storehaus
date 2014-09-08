package com.twitter.storehaus.cascading.split

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.util.Await
import com.twitter.storehaus.cascading.{ InitializableStoreObjectSerializer, Instance }
import org.apache.hadoop.io.Writable
import scala.collection.JavaConversions._
import java.net.InetAddress
import org.slf4j.{ Logger, LoggerFactory }

trait SplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q, T]] {
  def getSplittableStore(jobConf: JobConf): Option[SplittableStore[K, V, Q, T]] 
}

/**
 * SplittableStore based implementation of a SplittingMechanism
 */
class SplittableStoreSplittingMechanism[K, V, Q <: Writable, T <: SplittableStore[K, V, Q, T]](override val conf: JobConf) 
	extends StorehausSplittingMechanism[K, V](conf: JobConf) {
  @transient private val log = LoggerFactory.getLogger(classOf[SplittableStoreSplittingMechanism[K, V, Q, T]])
  
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val storeinit = InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[SplittableStoreCascadingInitializer[K, V, Q, T]]
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    log.debug(s"Getting splits for StorehausTap with id $tapid from SplittableStore")
    val splittableStore = storeinit.getSplittableStore(job).get
    val splittableStores = splittableStore.getSplits(hint)
    splittableStore.getInputSplits(splittableStores, tapid).asInstanceOf[Array[InputSplit]]
  }
  
  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    log.debug(s"Initializing Spool for StorehausTap with id $tapid")
    getStorehausSplit(split).spool = Some(storeinit.getSplittableStore(conf).get.
      getSplit(getStorehausSplit(split).getPredicate).getAll)
  }
  
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    val storesplit = getStorehausSplit(split)
    if(!storesplit.spool.isEmpty) {
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