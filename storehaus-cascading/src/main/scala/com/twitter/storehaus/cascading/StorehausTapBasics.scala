package com.twitter.storehaus.cascading

import cascading.tap.Tap
import org.apache.hadoop.mapred.{JobConf, RecordReader, OutputCollector}

trait StorehausTapBasics {
  def getModifiedTime(conf: JobConf) : Long = System.currentTimeMillis
  def createResource(conf: JobConf): Boolean = true
  def deleteResource(conf: JobConf): Boolean = true
  def resourceExists(conf: JobConf): Boolean = true
  
  type StorehausTapTypeInJava = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]
}