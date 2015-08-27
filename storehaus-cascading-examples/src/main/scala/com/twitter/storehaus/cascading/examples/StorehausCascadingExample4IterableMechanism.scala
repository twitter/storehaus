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
package com.twitter.storehaus.cascading.examples

import cascading.flow.FlowDef
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.pipe._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.tuple.Fields
import cascading.property.AppProps
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import java.util.{Properties, UUID}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import com.twitter.util.Await
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{ReadableStore, WritableStore, MapStore}
import com.twitter.storehaus.memcache.{MemcacheStore, MemcacheLongStore}
import com.twitter.storehaus.cascading.{StorehausCascadingInitializer, StorehausTap}
import com.twitter.storehaus.cascading.StorehausInputFormat
import com.twitter.storehaus.cascading.split._
import scala.language.implicitConversions
import scala.collection.mutable.HashMap


/**
 * Shows
 * - how to use IterableSplittableStore with MapStore and
 * - how to sink into Memcached
 * - working with reducers/every is straight forward, 
 *   as long as Cascading can serialize your data
 * - how to interpret the term in-memory-database :)
 */
class StoreInitializerMapStoreClass
    extends MapStoreClassInitializer {

  // prepare a simple, but bigger Map on the heap, might OOM
  val francis = """grant me the treasure of sublime poverty permit the distinctive sign of
    our order to be that it does not possess anything of its own beneath the sun for the 
    glory of your name and that it have no other patrimony than begging"""
  val textTokens = francis.split(" ")
  val map = HashMap[String, String]()
  for(keypart1 <- textTokens) {
    for(keypart2 <- textTokens) {
      for(keypart3 <- textTokens) {
        map += (s"$keypart1-$keypart2-$keypart3" -> UUID.randomUUID.toString)
      }
    }
  }
  
  // create a storehaus-in-memory-mapstore
  lazy val mapstore = new MapStore[String, String](map.toMap)
  // and use it as a SplittableStore
  lazy val splittableStore = new MapStoreClassSplittableIterableStore(mapstore, count = 30000l)
  
  override def getSplittableStore(jobConf: JobConf): Option[SplittableStore[String, String, LongWritable]] = 
      Some(splittableStore.asInstanceOf[SplittableStore[String, String, LongWritable]])
    
  override def prepareStore: Boolean = true
  
  override def getReadableStore(jobconf: JobConf): Option[ReadableStore[String, String]] =
    Some(mapstore.asInstanceOf[ReadableStore[String, String]])
  
  override def getWritableStore(jobconf: JobConf): Option[WritableStore[String, Option[String]]] = 
    Some(mapstore.asInstanceOf[WritableStore[String, Option[String]]])  
}

object StoreInitializerMapStore extends StoreInitializerMapStoreClass

/**
 * a memcache intitializer to sink the values
 */
object StoreInitializerMemcache
    extends StorehausCascadingInitializer[String, Long] {

  val store = new MemcacheLongStore(MemcacheStore(
      MemcacheStore.defaultClient("bla", ExampleConfigurationSettings.memcacheIpAddress)))
  
  override def prepareStore: Boolean = true

  override def getReadableStore(jobconf: JobConf): Option[ReadableStore[String, Long]] = 
    Some(store.asInstanceOf[ReadableStore[String, Long]])
  
  override def getWritableStore(jobconf: JobConf): Option[WritableStore[String, Option[Long]]] =    
    Some(store.asInstanceOf[WritableStore[String, Option[Long]]])
}

object StorehausCascadingIterableExample {
  
  val sumkey = "Sum"
  
  def main(args: Array[String]): Unit = {    
    val conf = new JobConf
    
    println(s"For control, mapstore has ${StoreInitializerMapStore.map.size} entries.")
    // using a SplittingMechanism usable with IterableStores
    // assuming that you have sets split-hint to two (or didn't change that value, so it will be 2, see conf.setNumMapTasks(int num)) 
    StorehausInputFormat.setSplittingClass[String, String, StoreInitializerMapStoreClass, MapStoreClassMechanism](conf, classOf[MapStoreClassMechanism])
    
    val properties = AppProps.appProps()
      .setName("StorehausCascadingIterableExample")
      .setVersion("0.0.1-SNAPSHOT")
      .buildProperties(conf)
    AppProps.setApplicationJarClass( properties, StorehausCascadingIterableExample.getClass )
    
    val flowConnector = new HadoopFlowConnector( properties )
    // create the source tap
    val inTap = new StorehausTap[String, String](StoreInitializerMapStore)
    // create the sink tap
    val outTap = new StorehausTap[String, Long](StoreInitializerMemcache)
    // specify some work to so
    val inPipe = new Pipe( "datapipe" )
    // add an integer we want to count
    val token = new Fields("token")
    val eachPipe = new Each(inPipe, new Insert(token, 1: java.lang.Integer), Fields.ALL)
    // group by this integer (prepare to count)
    val streamPipe = new GroupBy(eachPipe, token)
    // count all groups (which is only one group, so all items will be counted)
    val countPipe = new Every(streamPipe, Fields.ALL, new Count(), Fields.ALL)
    // make last field a string field, so we can use a memcachelongstore to sink the long (requires key to be a string)
    val replacePipe = new Each(countPipe, token, new Insert(token, sumkey), Fields.REPLACE)
    
    // connect the taps, pipes, etc., into a flow
    val flowDef = FlowDef.flowDef()
     .addSource( inPipe, inTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )
     .addTailSink( replacePipe, outTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )

    flowDef.setAssertionLevel(cascading.operation.AssertionLevel.STRICT)
    flowDef.setDebugLevel(cascading.operation.DebugLevel.VERBOSE)
    // run the flow
    val x = flowConnector.connect( flowDef )
    val y = x.complete()
    
    // present the result
    println("resulting count in memcache: " + Await.result(StoreInitializerMemcache.getReadableStore(conf).get.get(sumkey)).get)
    StoreInitializerMemcache.getReadableStore(conf).get.close()
    // for some reason this job doesn't stop by itself, so we exit the jvm
    System.exit(1)
  }
}
