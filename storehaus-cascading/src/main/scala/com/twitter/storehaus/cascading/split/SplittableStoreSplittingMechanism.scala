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

import com.twitter.storehaus.cascading.{ Instance, SplittableStore, StorehausCascadingInitializer, InitializableStoreObjectSerializer }
import com.twitter.storehaus.ReadableStore
import com.twitter.util.{ Try, Await }
import java.io.{ DataOutput, DataInput, ByteArrayOutputStream, ByteArrayInputStream }
import org.apache.hadoop.io.{ DefaultStringifier, WritableFactories, WritableFactory }
import org.apache.hadoop.io.serializer.{ Serialization, SerializationFactory, Serializer} 
import org.apache.hadoop.mapred.{ InputSplit, JobConf }
import scala.collection.mutable.ArrayBuffer

class SplittableStoreSplittingMechanism[K, V](override val conf: JobConf) 
    extends StorehausSplittingMechanism[K, V](conf) {
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val store = InitializableStoreObjectSerializer.getReadableStore(conf, tapid).get
    
  val DEFAULT_NUMBER_OF_MAPPERS = 1
  
  def getReadableStore: Option[ReadableStore[K, V]] = {
    store match {
      case readable: ReadableStore[K, V] => Some(readable)
      case _ => None
    }
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
    keys.map(keys => keys.grouped(getNumber(keys.size)).
        map(new KeyBasedSplit(_, conf)).toArray.asInstanceOf[Array[InputSplit]]).get
  }
  
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    split match {
      case ksplit: KeyBasedSplit[K] => 
        getReadableStore match {
          case Some(rstore) => 
        	  if (ksplit.getIterator.hasNext) {
        		val keyValue = ksplit.getIterator.next 
        	    key.set(keyValue)
        		Await.result(rstore.get(keyValue)).map(value.set(_))
        		true
        	  } else false
          case None => false
        }
      case _ => false 
    }
  }
  
  override def close = getReadableStore.map(store => store.close())
}

object SplittableStoreSplittingMechanism {
  val KEYARRAYCONFID = "com.twitter.storehaus.cascading.jobconfkeysplitting.data"
  val KEYCLASSCONFID = "com.twitter.storehaus.cascading.jobconfkeysplitting.class"
  /**
   * use the default serializers (e.g. Hadoop Writables) for the keys 
   * but it is also possible to register a different one in the job itself
   */
  def setKeyArray[K <: Object](conf: JobConf, keys: Array[K]) = {
    DefaultStringifier.storeArray(conf, keys, KEYARRAYCONFID)
  }
  def getKeyArray[K <: Object](conf: JobConf): Try[List[K]] = {
    Try {
        val clazz = Class.forName(conf.get(KEYCLASSCONFID)).asInstanceOf[Class[K]]
        DefaultStringifier.loadArray[K](conf, KEYARRAYCONFID, clazz).toList
    }
  }
}


