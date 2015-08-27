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
import java.util.concurrent.TimeUnit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import com.twitter.util.{Await, Duration}
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{ReadableStore, WritableStore, MapStore}
import com.twitter.storehaus.memcache.{MemcacheStore, MemcacheLongStore}
import com.twitter.storehaus.cascading.{StorehausCascadingInitializer, StorehausTap}
import com.twitter.storehaus.cascading.StorehausInputFormat
import com.twitter.storehaus.cascading.split._
import scala.language.implicitConversions
import scala.collection.mutable.HashMap
import org.slf4j._

/**
 * Shows
 * - how to use memcached as a source and a sink
 * - how to use a key-based-split which uses JobConf to process data
 */
class StoreInitializerKeyBasedMemcache
    extends StorehausCascadingInitializer[String, Long] {

  @transient val logger = LoggerFactory.getLogger(classOf[StoreInitializerKeyBasedMemcache])

  @transient lazy val store = {
      logger.info("Initializing memcache-store-client")
      val client = MemcacheStore.defaultClient("bla", ExampleConfigurationSettings.memcacheIpAddress, hostConnectionLimit = 10000)
      logger.info("Initializing memcache-store with memcache-store-client")
      val memstore = MemcacheStore(client)
      logger.info("Wrapping memcache-store as a Long store")
      new MemcacheLongStore(memstore)
  }
  override def prepareStore: Boolean = true
  override def getReadableStore(jobconf: JobConf): Option[ReadableStore[String, Long]] = {
    val result = Some(store.asInstanceOf[ReadableStore[String, Long]])
    logger.info("Got store")
    result
  }
  override def getWritableStore(jobconf: JobConf): Option[WritableStore[String, Option[Long]]] = 
    Some(store.asInstanceOf[WritableStore[String, Option[Long]]])
}

object StoreInitializerKeyBasedMemcache extends StoreInitializerKeyBasedMemcache

object StorehausCascadingMemcacheKeyBasedSplitExample {
  
  /**
   * used to add some data for testing; in a real setup data is probably 
   * already existing, so this will be skipped
   * 
   * However, important thing to note is that keys are being returned
   * to use them with the the KeyBasedSplittingMechanism
   */
  def putSomeDataForFun(count: Int, store: WritableStore[String, Option[Long]]): List[String] =
    1.to(count).map { number =>
      store.put(number.toString, Some(number))
      number.toString
    }.toList

  val sumkey = "sum"

  def main(args: Array[String]): Unit = {
    // fill in some values
    StoreInitializerKeyBasedMemcache.prepareStore
    val keys = putSomeDataForFun(10000, StoreInitializerKeyBasedMemcache.getWritableStore(null).get)

    val conf = new JobConf

    // Use a splitting mechanism that allows to read the keys from jobconf
    StorehausInputFormat.setSplittingClass[String, Long, StoreInitializerKeyBasedMemcache, 
      JobConfKeyArraySplittingMechanism[String, Long, StoreInitializerKeyBasedMemcache]](conf, 
          classOf[JobConfKeyArraySplittingMechanism[String, Long, StoreInitializerKeyBasedMemcache]])
    // For this splitting mechanism we need to place the keys in JobConf; so we'll need to append some serializers
    conf.set("io.serializations", "com.twitter.chill.hadoop.KryoSerialization," + conf.get("io.serializations"))
    JobConfKeyArraySplittingMechanism.setKeyArray(conf, keys.toArray, classOf[String])

    val properties = AppProps.appProps()
      .setName("StorehausCascadingMemcacheKeyBasedSplitExample")
      .setVersion("0.0.1-SNAPSHOT")
      .buildProperties(conf)
    AppProps.setApplicationJarClass( properties, StorehausCascadingMemcacheKeyBasedSplitExample.getClass )

    val valueField = new Fields("value")
    val keyField = new Fields("key")
    val sumField = new Fields(sumkey)

    val flowConnector = new HadoopFlowConnector( properties )
    // create the source tap
    val inTap = new StorehausTap[String, Long](StoreInitializerKeyBasedMemcache)
    // create the sink tap
    val outTap = new StorehausTap[String, Long](StoreInitializerKeyBasedMemcache)
    //val outTap = new Hfs(new TextDelimited(true, "\t"), "/tmp/testtestetst")
    // specify some work to so
    val inPipe = new Pipe( "datapipe" )
    // add an integer we want to groupby
    val token = new Fields("token")
    val eachPipe = new Each(inPipe, new Insert(token, 1: java.lang.Integer), Fields.ALL)
    // group by the value, which contains the Long
    val streamPipe = new GroupBy(eachPipe, token)
    // count all groups (which is only one group, so all items will be counted)
    val countPipe = new Every(streamPipe, token, new Sum(), Fields.RESULTS)
    // add a key we can query afterwards
    val insertKeyPipe = new Each(countPipe, sumField, new Insert(keyField, sumkey), new Fields("key", sumkey))
    // replace string field, so we can use a memcachelongstore to sink the long (requires key to be a string)
    val replacePipe = new Each(insertKeyPipe, new Fields("sum"), new Identity(new Fields("value")), new Fields("key", "value"))
    val typeCorrectedPipe = new Each(replacePipe, new Fields("value"), new Identity(java.lang.Long.TYPE), Fields.REPLACE)

    // connect the taps, pipes, etc., into a flow
    val flowDef = FlowDef.flowDef()
     .addSource( inPipe, inTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )
     .addTailSink( typeCorrectedPipe, outTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )

    flowDef.setAssertionLevel(cascading.operation.AssertionLevel.STRICT)
    flowDef.setDebugLevel(cascading.operation.DebugLevel.VERBOSE)
    // run the flow
    val x = flowConnector.connect( flowDef )
    val y = x.complete()

    // present the result
    println("resulting count in memcache: " + Await.result(StoreInitializerKeyBasedMemcache.getReadableStore(conf).get.get(sumkey)).get)
    println("result should be:" + 10000)
    StoreInitializerKeyBasedMemcache.getReadableStore(conf).get.close(Duration(60, TimeUnit.SECONDS).fromNow)
  }
}
