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
package com.twitter.storehaus.cascading.versioned

import com.twitter.storehaus.{ ReadableStore , WritableStore }
import com.twitter.storehaus.cascading.{StorehausTapBasics, Instance}
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
class VersionedStorehausTap[K, V](@transient store: VersionedStorehausCascadingInitializer[K, V], version: Long)
  extends Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]](new VersionedStorehausScheme(store, version)) 
  with StorehausTapBasics {
    
  override def openForRead(process: FlowProcess[JobConf], 
      input: RecordReader[Instance[K], Instance[V]]): TupleEntryIterator = { 
    new HadoopTupleEntrySchemeIterator(process, this, input)
  }

  override def openForWrite(process: FlowProcess[JobConf], 
      output: OutputCollector[K, V]): TupleEntryCollector = { 
    new HadoopTupleEntrySchemeCollector(process, this.asInstanceOf[StorehausTapTypeInJava])
  }
  
  private val id: String = getScheme.asInstanceOf[VersionedStorehausScheme[K, V]].getId
  
  override def getIdentifier: String = this.id
  override def equals(o: Any): Boolean =
    o.getClass.equals(this.getClass) && id.equals(o.asInstanceOf[VersionedStorehausTap[K, V]].id)
}
