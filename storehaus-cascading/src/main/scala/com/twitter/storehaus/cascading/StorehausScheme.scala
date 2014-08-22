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

import com.twitter.storehaus.{ ReadableStore, WritableStore }
import com.twitter.util.Future
import java.util.UUID
import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple, TupleEntry }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

/**
 * Scheme can be instantiated on stores conforming to Readable- or WritableStore or both
 */
class StorehausScheme[K, V]
  (@transient store: StorehausCascadingInitializer[K, V], id: String = UUID.randomUUID.toString)
  extends Scheme[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V], Seq[Object], Seq[Object]](/*new Fields("key", "value")*/) {

  def getId = this.id
 
  override def source(process : FlowProcess[JobConf],
    sourceCall : SourceCall[Seq[Object], RecordReader[Instance[K], Instance[V]]]) : Boolean = {
    val key : Instance[K] = sourceCall.getContext()(0).asInstanceOf[Instance[K]]
    val value : Instance[V] = sourceCall.getContext()(1).asInstanceOf[Instance[V]]
    val result = sourceCall.getInput.next(key, value)
    if (result) {
      val tuple = new Tuple
      // fixing fields to key and value
      tuple.add(key.get)
      tuple.add(value.get)
      sourceCall.getIncomingEntry.setTuple(tuple)
    }
    result
  }

  /**
   * serializing the store and setting InputFormat 
   */
  override def sourceConfInit(process : FlowProcess[JobConf],
    tap : Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]],
    conf : JobConf) : Unit = {
    InitializableStoreObjectSerializer.setTapId(conf, getId)
    InitializableStoreObjectSerializer.setReadableStoreClass(conf, getId, store)
    conf.setInputFormat(classOf[StorehausInputFormat[K, V]])
  }

  /**
   * creating key and value of type Instance, such that we can fill in anything we want 
   */
  override def sourcePrepare(process : FlowProcess[JobConf],
      sourceCall : SourceCall[Seq[Object], RecordReader[Instance[K], Instance[V]]]) : Unit = {
    sourceCall.setContext(List(
      sourceCall.getInput.createKey.asInstanceOf[Object],
      sourceCall.getInput.createValue.asInstanceOf[Object]
    ))
  }

  override def sourceCleanup(process : FlowProcess[JobConf],
    sourceCall : SourceCall[Seq[Object], RecordReader[Instance[K], Instance[V]]]) : Unit = {
    // TODO: check if we need to let go of anything else here
    sourceCall.setContext(null)
  }

  /**
   * serializing the store constructor params and setting OutputFormat 
   */
  override def sinkConfInit(process : FlowProcess[JobConf],
      tap : Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]],
      conf : JobConf) : Unit = {
    InitializableStoreObjectSerializer.setTapId(conf, getId)
    InitializableStoreObjectSerializer.setWritableStoreClass(conf, getId, store)
    conf.setOutputFormat(classOf[StorehausOutputFormat[K, V]])
  }

  /**
   * we require that our Tuple contains a "key" in position 0 and a "value" in position 1  
   */
  override def sink(process : FlowProcess[JobConf],
    sinkCall : SinkCall[Seq[Object], OutputCollector[K, V]]) : Unit = {
    val tuple = sinkCall.getOutgoingEntry()
    sinkCall.getOutput().collect(tuple.getObject(0).asInstanceOf[K], tuple.getObject(1).asInstanceOf[V])
  }

  override def equals(o : Any) : Boolean =
    o.getClass.equals(this.getClass) && id.equals(o.asInstanceOf[StorehausScheme[K, V]].getId)
}
