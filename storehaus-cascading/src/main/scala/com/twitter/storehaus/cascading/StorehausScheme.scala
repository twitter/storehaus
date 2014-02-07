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

import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future

import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple, TupleEntry }

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }

class StorehausSourceScheme[K, V](store : ReadableStore[K, V],
  keySet : Set[K], fields : Fields, id : String)
  (implicit fn: ((K, Future[Option[V]])) => Tuple)
  extends Scheme[JobConf, RecordReader[KVWrapper[K, V], NullWritable], OutputCollector[_,_], Seq[Object], Seq[Object]](fields) {

  def getId = this.id

  override def source(process : FlowProcess[JobConf],
    sourceCall : SourceCall[Seq[Object], RecordReader[KVWrapper[K, V], NullWritable]]) : Boolean = {

    val key : KVWrapper[K, V] = sourceCall.getContext()(0).asInstanceOf[KVWrapper[K, V]]
    val value : NullWritable = sourceCall.getContext()(1).asInstanceOf[NullWritable]

    sourceCall.getInput.next(key, value) match {
      case false => false
      case true => sourceCall.getIncomingEntry.setTuple(fn(key.tuple)); true
    }
  }

  override def sourceConfInit(process : FlowProcess[JobConf],
    tap : Tap[JobConf, RecordReader[KVWrapper[K, V], NullWritable], OutputCollector[_,_]],
    conf : JobConf) : Unit = {
    conf.setInputFormat(classOf[StorehausInputFormat[K, V]])
  }

  override def sourcePrepare(process : FlowProcess[JobConf],
      sourceCall : SourceCall[Seq[Object], RecordReader[KVWrapper[K, V], NullWritable]]) : Unit = {
    sourceCall.setContext(List(
      sourceCall.getInput.createKey.asInstanceOf[Object],
      sourceCall.getInput.createValue.asInstanceOf[Object]
    ))
  }

  override def sourceCleanup(process : FlowProcess[JobConf],
    sourceCall : SourceCall[Seq[Object], RecordReader[KVWrapper[K, V], NullWritable]]) : Unit = {
    // TODO: check if we need to let go of anything else here
    sourceCall.setContext(null)
  }

  override def sinkConfInit(process : FlowProcess[JobConf],
    tap : Tap[JobConf, RecordReader[KVWrapper[K, V], NullWritable], OutputCollector[_,_]],
    conf : JobConf) : Unit = {
    throw new UnsupportedOperationException("Not supported.")
  }

  override def sink(process : FlowProcess[JobConf],
    sinkCall : SinkCall[Seq[Object], OutputCollector[_,_]]) : Unit = {
    throw new UnsupportedOperationException("Not supported.")
  }

  override def equals(o : Any) : Boolean =
    o.getClass.equals(this) && id.equals(o.asInstanceOf[StorehausSourceScheme[K, V]].getId)
}
