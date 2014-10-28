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
package com.twitter.storehaus.cascading

import org.apache.hadoop.mapred.{ OutputFormat, JobConf, RecordWriter, Reporter }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.Progressable
import org.slf4j.{ Logger, LoggerFactory }
import com.twitter.storehaus.WritableStore
import com.twitter.util.Await

/**
 * StorehausOuputFormat using a WriteableStore
 */
class StorehausOutputFormat[K, V] extends OutputFormat[K, V] {  
  @transient private val log = LoggerFactory.getLogger(classOf[StorehausOutputFormat[K, V]])
  val FORCE_FUTURE_IN_OUTPUTFORMAT = "com.twitter.storehaus.cascading.outputformat.forcefuture"

  /**
   * Simple StorehausRecordWriter delegating method-calls to store 
   */
  class StorehausRecordWriter(val conf: JobConf) extends RecordWriter[K, V] {  
    var store: Option[WritableStore[K, Option[V]]] = None
    override def write(key: K, value: V) = {
      val tapid = InitializableStoreObjectSerializer.getTapId(conf)      
      store = if(store.isEmpty) {
        log.debug(s"RecordWriter will initialize the store.")
        InitializableStoreObjectSerializer.getWriteVerion(conf, tapid) match {
          case None => {
            InitializableStoreObjectSerializer.getWritableStore[K, Option[V]](conf, tapid)
          }
          case Some(version) => InitializableStoreObjectSerializer.getWritableVersionedStore[K, Option[V]](conf, tapid, version)
        }
      }.onFailure(e => log.error(s"RecordWriter was not able to initialize the store for tap $tapid.", e)).toOption else store
      log.debug(s"RecordWriter writing value=$value for key=$key into ${store.get}.")
      // handle with care - make sure thread pools shut down TPEs on used stores correctly if asynchronous
      // that includes awaitTermination and adding shutdown hooks, depending on mode of operation of Hadoop
      if (conf.get(FORCE_FUTURE_IN_OUTPUTFORMAT) != null && conf.get(FORCE_FUTURE_IN_OUTPUTFORMAT).equalsIgnoreCase("true"))
        store.get.put((key, Some(value)))
      else
        Await.result(store.get.put((key, Some(value))))
    }
    override def close(reporter: Reporter) = {
      log.debug(s"RecordWriter finished. Closing.")
      store.map(_.close())
      reporter.setStatus("Completed Writing. Closed Store.")
    }
  }

  /**
   * initializes a WritableStore out of serialized JobConf parameters and returns a RecordWriter 
   * putting into that store.
   */
  override def getRecordWriter(fs: FileSystem, conf: JobConf, name: String, progress: Progressable): RecordWriter[K, V] = {
    // log.debug(s"Returning RecordWriter, retrieved store from StoreInitializer ${InitializableStoreObjectSerializer.getWritableStoreIntializer(conf, tapid).get.getClass.getName} by reflection: ${store.toString()}")
    new StorehausRecordWriter(conf)
  }

  override def checkOutputSpecs(fs: FileSystem, conf: JobConf) = {}

}