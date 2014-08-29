package com.twitter.storehaus.cascading.examples

import cascading.flow.FlowDef
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.pipe.Pipe
import cascading.property.AppProps
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs

import java.util.Properties

import org.apache.hadoop.mapred.JobConf

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.cassandra.cql.{ CQLCassandraCollectionStore, CQLCassandraStore }
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraCompositeStore
import com.twitter.storehaus.{ReadableStore, WritableStore}
import com.twitter.storehaus.cascading.{ StorehausCascadingInitializer, StorehausTap }


import scala.language.implicitConversions
import shapeless._
import HList._
import Traversables._
import Nat._
import UnaryTCConstraint._

import java.util.Date

import com.websudos.phantom.CassandraPrimitive

import scala.reflect.runtime.universe._
import scala.reflect._

/**
 * demonstrates usage of Cassandra in the Storehaus-Tap
 */
object SimpleStoreInitializer extends StorehausCascadingInitializer[String, String] {

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
  val columnFamily = StoreColumnFamily("ColumnFamilyNameForTestingSimple", 
      StoreSession("mytestkeyspace", StoreCluster("Test Cluster", Set(StoreHost("localhost")))))
  
  // create store
  val store = new CQLCassandraStore[String, String](columnFamily)
  
  override def prepareStore: Boolean = {
    CQLCassandraStore.createColumnFamily[String, String](columnFamily)
    true
  }
  
  override def getReadableStore(jobconf: JobConf): Option[ReadableStore[String, String]] = {    
    Some(store.asInstanceOf[ReadableStore[String, String]])
  }
  
  override def getWritableStore(jobconf: JobConf): Option[WritableStore[String, Option[String]]] = {    
    Some(store.asInstanceOf[WritableStore[String, Option[String]]])
  }  
}

/**
 * demonstrates simple Cascading flow using a StorehausTap as a sink
 */
object StorehausCascadingStandaloneTestSimple {
  
  /**
   * see http://docs.cascading.org/impatient/impatient1.html
   * changed in- and output taps
   */
  def main(args: Array[String]): Unit = {
    SimpleStoreInitializer.prepareStore
    
    val properties = new Properties()
    AppProps.setApplicationJarClass( properties, StorehausCascadingStandaloneTest.getClass )
    
    val flowConnector = new HadoopFlowConnector( properties )
    // create the source tap
    val inTap = new Hfs( new TextDelimited( true, "\t" ), "/tmp/test.in" )

    // create the sink tap
    val outTap =  new StorehausTap[String, String](SimpleStoreInitializer)

    // specify a pipe to connect the taps
    val copyPipe = new Pipe( "copy" )
    
    // connect the taps, pipes, etc., into a flow
    val flowDef = FlowDef.flowDef()
     .addSource( copyPipe, inTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )
     .addTailSink( copyPipe, outTap.asInstanceOf[cascading.tap.Tap[_,_,_]] )

    flowDef.setAssertionLevel(cascading.operation.AssertionLevel.STRICT)
    flowDef.setDebugLevel(cascading.operation.DebugLevel.VERBOSE)
    // run the flow
    val x = flowConnector.connect( flowDef )
    val y = x.complete()
    
  }
}
