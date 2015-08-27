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
package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.cascading.{ AbstractStorehausCascadingInitializer, Instance, InitializableStoreObjectSerializer }
import com.twitter.storehaus.ReadableStore
import com.twitter.util.{ Try, Await }
import org.apache.hadoop.io.DefaultStringifier
import org.apache.hadoop.mapred.{ InputSplit, JobConf }
import org.slf4j.LoggerFactory

/**
 * Reads a stream of keys from the JobConf and eventually from a storehaus store.
 * 
 * Be aware that all keys to perform the "get" operation upon (storehaus ReadableStore)
 * will be serialized, stored and transmitted over the wire using JobConf. 
 * The base class StorehausSplittingMechanism is intended to be extended for cases which
 * require e.g. a lookup in a store. 
 * 
 * Because keys must be provided this splitting mechanism is not for versioning stores.
 */
class JobConfKeyArraySplittingMechanism[K, V, U <: AbstractStorehausCascadingInitializer](override val conf: JobConf) 
    extends StorehausSplittingMechanism[K, V, U](conf) {
  @transient val log = LoggerFactory.getLogger(classOf[JobConfKeyArraySplittingMechanism[K, V, U]])
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  lazy val store = InitializableStoreObjectSerializer.getReadableStore[K, V](conf, tapid)
  val DEFAULT_NUMBER_OF_MAPPERS = 1
  @transient private var keyIterator: Option[Iterator[K]] = None

  lazy val readableStore: ReadableStore[K, V] = {
    store.onFailure { e => log.error("Error intializing store: " + e); e.printStackTrace; throw e }
    store.toOption.get
  }

  override def getSplits(conf: JobConf, hint: Int) : Array[InputSplit] = {
    def getNumber(size: Int) = {
      val numMapper = hint match {
        case 0 => {
          if(conf.getNumMapTasks() > 0) 
            conf.getNumMapTasks()
          else
            DEFAULT_NUMBER_OF_MAPPERS
        }
        case hinted if hint > 0 => hinted
        case _ => DEFAULT_NUMBER_OF_MAPPERS
      } 
      if ((size / numMapper) > 0) 
        size / numMapper 
      else if (size == 0)
        1
        else 
          size
    }
    val keys = JobConfKeyArraySplittingMechanism.getKeyArray(conf)
    keys.map(klist => log.info("Number of keys: " + klist.length))
    val res = keys.map(keys => keys.grouped(getNumber(keys.size)).
        map(new KeyBasedSplit(_, Some(conf), JobConfKeyArraySplittingMechanism.
            getKeyClassFromConf[K](conf).toOption)).toArray.asInstanceOf[Array[InputSplit]]).get
    log.info("Number of splits: " + res.length)
    res
  }

  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    split match {
      case ksplit: KeyBasedSplit[K] =>
        if(keyIterator.isEmpty) {
          keyIterator = Some(ksplit.getIterator)
        }
        keyIterator.map{it =>
          if (it.hasNext) {
            val keyValue = it.next
            key.set(keyValue)
            Await.result(readableStore.get(keyValue)).map(value.set(_))
            log.info("key/value:" + keyValue + ":" + value.get)
            true
          } else {
            log.info("Split contains no more keys to get from store. Reading complete.")
            false
          }
        }.getOrElse(false)
      case _ => false
    }
  }

  override def close() = store.toOption.map(rstore => rstore.close())
}

object JobConfKeyArraySplittingMechanism {
  val KEYARRAYCONFID = "com.twitter.storehaus.cascading.jobconfkeysplitting.data"
  val KEYCLASSCONFID = "com.twitter.storehaus.cascading.jobconfkeysplitting.class"
  /**
   * use the default serializers (e.g. Hadoop Writables) for the keys 
   * but it is also possible to register a different one in the job itself
   */
  def setKeyArray[K <: Object](conf: JobConf, keys: Array[K], clazz: Class[K]) = {
    conf.set(KEYCLASSCONFID, clazz.getName)
    DefaultStringifier.storeArray(conf, keys, KEYARRAYCONFID)
  }
  def getKeyArray[K <: Object](conf: JobConf): Try[List[K]] = {
    Try {
        DefaultStringifier.loadArray[K](conf, KEYARRAYCONFID, getKeyClassFromConf[K](conf).get).toList
    }
  }
  def getKeyClassFromConf[K](conf: JobConf): Try[Class[K]] =
    Try(Class.forName(conf.get(KEYCLASSCONFID)).asInstanceOf[Class[K]])
}
