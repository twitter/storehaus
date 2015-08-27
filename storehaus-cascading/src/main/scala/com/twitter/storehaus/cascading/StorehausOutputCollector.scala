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
package com.twitter.storehaus.cascading

import cascading.tuple.TupleEntrySchemeCollector
import org.apache.hadoop.mapred.{RecordReader, OutputCollector}
import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import com.twitter.util.Try
import cascading.tap.TapException
import cascading.scheme.ConcreteCall
import cascading.scheme.SinkCall
import cascading.scheme.Scheme
import cascading.tuple.TupleEntry

class StorehausOutputCollector[K, V, OutputT](flowProcess: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[K, V], OutputCollector[K, V]]) 
  extends TupleEntrySchemeCollector[JobConf, OutputT](flowProcess, tap.getScheme()) /* with OutputCollector[JobConf, OutputT]*/ {

  @transient val logger = LoggerFactory.getLogger(classOf[StorehausOutputCollector[K, V, OutputT]])
  @transient lazy val conf = flowProcess.getConfigCopy
  @transient lazy val outputFormat = new StorehausOutputFormat[K, V]
  @transient lazy val writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier, Reporter.NULL)
  @transient lazy val scheme = tap.getScheme.asInstanceOf[Scheme[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V], Seq[Object], Seq[Object]]]
  
  @transient lazy val keepaliveinterval = StorehausOutputCollector.getKeepaliveRepeatInterval(conf)
  @transient var count = 0L
  
  logger.info("initialized StorehausOutputCollector")

  override def prepare(): Unit = {
    logger.info("preparing StorehausOutputCollector")
    tap.sinkConfInit(flowProcess, conf)
    super.prepare
  }
  
  override def collect(tupleEntry: TupleEntry): Unit = {
    if(count % keepaliveinterval == 0L) {
      flowProcess.keepAlive()
    }
    // sinkCall.setOutgoingEntry( tupleEntry )
    // scheme.sink(flowProcess, sinkCall.asInstanceOf[SinkCall[Seq[Object], OutputCollector[K, V]]])
    writer.write(tupleEntry.getObject(0).asInstanceOf[K], tupleEntry.getObject(1).asInstanceOf[V]) 
  }
  
  override def close(): Unit = {
    super.close()
    logger.info("closing StorehausOutputCollector for: {}", tap)
    Try(writer.close(Reporter.NULL)).onFailure { throwable => 
      logger.warn(s"Error closing StorehausOutputCollector for ${tap}")
      throw new TapException("exception closing: ", throwable)
    }
  }
}

object StorehausOutputCollector {
  val KEEP_ALIVE_REPEAT_INTERVAL = "com.twitter.storehaus.cascading.keepalive.interval"
  
  def setKeepaliveRepeatInterval(conf: JobConf, interval: Long): Unit = conf.set(KEEP_ALIVE_REPEAT_INTERVAL, interval.toString())
  def getKeepaliveRepeatInterval(conf: JobConf): Long = Try(conf.get(KEEP_ALIVE_REPEAT_INTERVAL).toLong).toOption.getOrElse(1000L)
}
