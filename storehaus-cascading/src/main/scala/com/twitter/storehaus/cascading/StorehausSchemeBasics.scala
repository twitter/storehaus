package com.twitter.storehaus.cascading

import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple, TupleEntry }

/**
 * trait to remove code duplication in storehaus-schemes
 */ 
trait StorehausSchemeBasics[K, V] {

  def getId: String

  def setReadStore(conf: JobConf)

  def setWriteStore(conf: JobConf)
  
  def source(process : FlowProcess[JobConf],
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
  def sourceConfInit(process : FlowProcess[JobConf],
    tap : Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]],
    conf : JobConf) : Unit = {
    InitializableStoreObjectSerializer.setTapId(conf, getId)
    setReadStore(conf)
    conf.setInputFormat(classOf[StorehausInputFormat[K, V, AbstractStorehausCascadingInitializer]])
  }

  /**
   * creating key and value of type Instance, such that we can fill in anything we want 
   */
  def sourcePrepare(process : FlowProcess[JobConf],
      sourceCall : SourceCall[Seq[Object], RecordReader[Instance[K], Instance[V]]]) : Unit = {
    sourceCall.setContext(List(
      sourceCall.getInput.createKey.asInstanceOf[Object],
      sourceCall.getInput.createValue.asInstanceOf[Object]
    ))
  }

  def sourceCleanup(process : FlowProcess[JobConf],
    sourceCall : SourceCall[Seq[Object], RecordReader[Instance[K], Instance[V]]]) : Unit = {
    // TODO: check if we need to let go of anything else here
    sourceCall.setContext(null)
  }
  
  /**
   * serializing the store constructor params and setting OutputFormat 
   */
  def sinkConfInit(process : FlowProcess[JobConf],
      tap : Tap[JobConf, RecordReader[Instance[K], Instance[V]], OutputCollector[K, V]],
      conf : JobConf) : Unit = {
    InitializableStoreObjectSerializer.setTapId(conf, getId)
    setWriteStore(conf)
    conf.setOutputFormat(classOf[StorehausOutputFormat[K, V]])
  }

  /**
   * we require that our Tuple contains a "key" in position 0 and a "value" in position 1  
   */
  def sink(process : FlowProcess[JobConf],
    sinkCall : SinkCall[Seq[Object], OutputCollector[K, V]]) : Unit = {
    val tuple = sinkCall.getOutgoingEntry()
    sinkCall.getOutput().collect(tuple.getObject(0).asInstanceOf[K], tuple.getObject(1).asInstanceOf[V])
  }

}