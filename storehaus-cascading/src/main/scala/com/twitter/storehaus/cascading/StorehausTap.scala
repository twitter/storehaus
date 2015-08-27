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
import org.slf4j.LoggerFactory

/**
 * Cascading-Tap for ReadableStore and WritableStore.
 * 
 * Initializing stores is done in In- and OutputFormat and may require 
 * constructor parameters of the stores to be serialized to JobConf.
 * 
 * It is highly advisable to use a custom or store-specific version of a 
 * SplittingMechanism which knows how to either process custom data or issue
 * queries against the store to retrieve sets of keys.  
 * 
 * @author Ruban Monu
 * @author Andreas Petter
 */
class StorehausTap[K, V](@transient store: StorehausCascadingInitializer[K, V], useStorehausCollector: Boolean = true)
  extends Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]](new StorehausScheme(store)) 
  with StorehausTapBasics {
    
  @transient lazy val logger = LoggerFactory.getLogger(classOf[StorehausTap[K, V]])
  
  override def openForRead(process: FlowProcess[JobConf], 
      input: RecordReader[Instance[K], Instance[V]]): TupleEntryIterator = { 
    new HadoopTupleEntrySchemeIterator(process, this, input)
  }

  override def openForWrite(process: FlowProcess[JobConf], 
      output: OutputCollector[K, V]): TupleEntryCollector = {
    useStorehausCollector match {
      case true => 
        logger.info(s"Opening StorehausOutputCollector as OutputCollector")
        new StorehausOutputCollector(process, this.asInstanceOf[Tap[JobConf, RecordReader[Any, Any], OutputCollector[Any, Any]]])
      case false => 
        logger.info(s"Opening HadoopTupleEntrySchemeCollector as OutputCollector")
        new HadoopTupleEntrySchemeCollector(process, this.asInstanceOf[StorehausTapTypeInJava])
    }
  }
  
  private val id: String = getScheme.asInstanceOf[StorehausScheme[K, V]].getId
  
  override def getIdentifier: String = this.id
  override def equals(o: Any): Boolean =
    o.getClass.equals(this.getClass) && id.equals(o.asInstanceOf[StorehausTap[K, V]].id)
}
