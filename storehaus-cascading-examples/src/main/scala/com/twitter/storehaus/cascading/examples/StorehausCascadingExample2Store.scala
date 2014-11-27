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
import com.twitter.storehaus.cassandra.cql.{ AbstractCQLCassandraCompositeStore, CQLCassandraCollectionStore, CQLCassandraStore }
import com.twitter.storehaus.cassandra.cql.cascading.{ CassandraCascadingInitializer, CassandraCascadingRowMatcher }
import com.twitter.storehaus.{ReadableStore, WritableStore}
import com.twitter.storehaus.cascading.{ StorehausCascadingInitializer, StorehausTap }

import scala.language.implicitConversions
import shapeless._
import HList._
import ops.hlist.Mapper
import ops.hlist.Mapped
import ops.hlist.ToList
import Nat._
import UnaryTCConstraint._


import java.util.Date

import com.websudos.phantom.CassandraPrimitive

import scala.reflect.runtime.universe._
import scala.reflect._


/**
 * default name for the object used by StorehausTaps is StoreInitializer
 * 
 * May not use state information other than JobConf.
 */
object StoreInitializer 
    extends StorehausCascadingInitializer[(String :: Long :: HNil, Long :: Date :: HNil), String] 
    with CassandraCascadingInitializer[(String :: Long :: HNil, Long :: Date :: HNil), String] {
  // this example StoreInitializer is for a storehaus-cassandra store
  // for storehaus-cassandra type parameters are HLists
  
  
  // setup basic store parameters (special to storehaus-cassandra, setup is different for other stores)
  import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
  
  override def getColumnFamilyName(version: Option[Long]) = "ColumnFamilyNameForTesting"
  override def getKeyspaceName = ExampleConfigurationSettings.cassandraKeySpaceName
  override def getThriftConnections = ExampleConfigurationSettings.cassandraIpAddress + ":" + ExampleConfigurationSettings.cassandraThriftPort
    
  val columnFamily = StoreColumnFamily(getColumnFamilyName(None), 
      StoreSession(getKeyspaceName, StoreCluster("Test Cluster", Set(StoreHost(ExampleConfigurationSettings.cassandraIpAddress)))))
  
  
  // setup key serializers for HLists by example (special to storehaus-cassandra, setup is different for other stores)
  val rs = AbstractCQLCassandraCompositeStore.getSerializerHListByExample("TestingExampleKey" :: 1234l :: HNil)
  val cs = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(12345l :: new Date :: HNil)
  
  // setup column names (special to storehaus-cassandra, setup is different for other stores) and a type of values
  val rowKeyNames = List("name", "somenumber")
  val colKeyNames = List("othernumber", "sometime")
  val valueSerializer = implicitly[CassandraPrimitive[String]]
  
  // create store
  val store = new CQLCassandraCollectionStore[String :: Long :: HNil, Long :: Date :: HNil, Set[String], String, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Long] :: HNil, CassandraPrimitive[Long] :: CassandraPrimitive[Date] :: HNil](
	      columnFamily, rs, rowKeyNames, cs, colKeyNames)(implicitly[Semigroup[Set[String]]]) 
  
  /**
   * store prepare, do this similar for other storehaus-stores
   */
  override def prepareStore: Boolean = {
    // creates keyspace and columnfamily
    CQLCassandraCollectionStore.createColumnFamily(columnFamily, rs, rowKeyNames, cs, colKeyNames, valueSerializer, Set[String]())
    true
  }
  
  /**
   * used to intialize the store, do this similar for other storehaus-stores
   */
  override def getReadableStore(jobconf: JobConf): Option[ReadableStore[(String :: Long :: HNil, Long :: Date :: HNil), String]] = {    
    // automatic type conversion doesn't seem to work here
    Some(store.asInstanceOf[ReadableStore[(String :: Long :: HNil, Long :: Date :: HNil), String]])
  }
  
  /**
   * used to intialize the store, do this similar for other storehaus-stores
   */
  override def getWritableStore(jobconf: JobConf): Option[WritableStore[(String :: Long :: HNil, Long :: Date :: HNil), Option[String]]] = {    
    // automatic type conversion doesn't seem to work here
    Some(store.asInstanceOf[WritableStore[(String :: Long :: HNil, Long :: Date :: HNil), Option[String]]])
  }
  
  // Cassandra specific method used to transform rows from the cassandra hadoop input format into keys and values
  override def getCascadingRowMatcher = store.asInstanceOf[CassandraCascadingRowMatcher[(String :: Long :: HNil, Long :: Date :: HNil), String]]
}

object StorehausCascadingExample2Store {
  
  /**
   * used to add some data for testing; in a real setup data is probably 
   * already existing, so this can be skipped
   */
  def putSomeDataForFun() = {
    StoreInitializer.prepareStore
    val store = StoreInitializer.getWritableStore(null)
  }
  
  /**
   * see http://docs.cascading.org/impatient/impatient1.html
   * changed in- and output taps
   */
  def main(args: Array[String]): Unit = {
    StoreInitializer.prepareStore
    
    val properties = new Properties()
    AppProps.setApplicationJarClass( properties, StorehausCascadingExample2Store.getClass )
    
    val flowConnector = new HadoopFlowConnector( properties )
    // create the source tap
    val inTap = new StorehausTap[(String :: Long :: HNil, Long :: Date :: HNil), String](StoreInitializer)

    // create the sink tap
    val outTap = new StorehausTap[(String :: Long :: HNil, Long :: Date :: HNil), String](StoreInitializer)

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
