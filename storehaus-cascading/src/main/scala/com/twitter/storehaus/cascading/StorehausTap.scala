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

import com.twitter.storehaus.{ ReadableStore , WritableStore }
import com.twitter.util.Future
import cascading.flow.FlowProcess
import cascading.tap.hadoop.io.{ HadoopTupleEntrySchemeIterator, HadoopTupleEntrySchemeCollector }
import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple, TupleEntryIterator, TupleEntryCollector }
import java.util.Date
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{ JobConf, RecordReader, OutputCollector }

/**
 * Cascading-Tap for ReadableStore and WritableStore.
 * 
 * Initiatilizing stores is done in In- and OutputFormat and may require 
 * constructor parameters of the stores to be serialized to JobConf.
 * 
 * It is highly advisable to use a custom or store-specific version of a 
 * SplittingMechanism which knows how to either process custom data or issue
 * queries against the store to retrieve sets of keys.  
 * 
 * @author Ruban Monu
 * @author Andreas Petter
 */
class StorehausTap[K, V](@transient store: StorehausCascadingInitializer[K, V], @transient version: Option[Long] = None)
  extends Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]](new StorehausScheme(store, version)) {
  
  override def openForRead(process: FlowProcess[JobConf], 
      input: RecordReader[Instance[K], Instance[V]]): TupleEntryIterator = { 
    new HadoopTupleEntrySchemeIterator(process, this, input)
  }

  // used to satisfy the Scala compiler and circumvent Scala<->Java compatibility issues
  private type StorehausTapTypeInJava = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]  

  override def openForWrite(process: FlowProcess[JobConf], 
      output: OutputCollector[K, V]): TupleEntryCollector = { 
    new HadoopTupleEntrySchemeCollector(process, this.asInstanceOf[StorehausTapTypeInJava])
  }
  
  private val id: String = getScheme.asInstanceOf[StorehausScheme[K, V]].getId
  
  override def getIdentifier: String = this.id
  override def getModifiedTime(conf: JobConf) : Long = System.currentTimeMillis
  override def createResource(conf: JobConf): Boolean = true
  override def deleteResource(conf: JobConf): Boolean = true
  override def resourceExists(conf: JobConf): Boolean = true
  override def equals(o: Any): Boolean =
    o.getClass.equals(this.getClass) && id.equals(o.asInstanceOf[StorehausTap[K, V]].id)
}
