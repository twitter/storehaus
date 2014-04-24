/*
* Copyright 2013 SEEBURGER AG
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License. You may obtain
* a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.twitter.storehaus.cassandra

import com.twitter.util.{ Await, Duration, Try }
import org.scalacheck.{ Gen, Properties }
import org.scalacheck.Prop.forAll
import me.prettyprint.cassandra.service.ThriftKsDef
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.factory.HFactory
import java.math.BigInteger
import java.util.concurrent.TimeUnit

/** Unit test using Long values */
object CassandraStoreProperties extends Properties("CassandraStore") {

  val hostname = "localhost:9160"
  val clusterName = "Test Cluster"
  val keyspaceName = "Myspace"
  val countercf = "countercf"
  val dyncf = "dynamiccompositecf"
  val comcf = "compositecf"
  val storecf = "kvstorecf"

  /**
   * test dynamic composite store.
   * Splits keys into two parts using a Tuple2: first part is row-key, second is col-key  
   */
  def putAndGetDynamicComposite(cluster: CassandraConfiguration.StoreCluster, keyspace: CassandraConfiguration.StoreKeyspace) = {
	import ScalaSerializables._
    
	val columnFamily = new CassandraConfiguration.StoreColumnFamily(dyncf)

	// sets up columnfamily, type param = type of values being putted in
	CassandraDynamicCompositeStore.setupStore[String](cluster, keyspaceName, columnFamily)
    
	// intialize store
	val store = new CassandraDynamicCompositeStore[String](keyspace, columnFamily)
    
    val expected = Some("string-test")
    val key = (123L, List("test", 1l, 2.3d))
    
    Await.result(store.put(key, expected))
    val result = Await.result(store.get(key))

    // --- now test PartiallyIterableStore
    // some more columns... to have s.th. to query 
    val cols1 = new BigInteger("234") :: "Antwerp" :: 2l :: Nil 
    val cols2 = new BigInteger("234") :: "Berlin" :: 6l :: Nil 
    val cols3 = new BigInteger("234") :: "Chicago" :: 1l :: Nil 
    Await.result(store.put(((key, cols1), "Netherlands")))    
    Await.result(store.put(((key, cols2), "Germany")))
    Await.result(store.put(((key, cols3), "United States of America")))
    
    val start = (key, cols1)
    val end = Some((key, new BigInteger("234") :: "Berlin" :: 7l :: Nil ))
    // we have an implicit to order our keys; the ordering is not implied by Cassandra
    val result2 =  Await.result(store.multiGet(start, end))
    
    (result == expected) && (result2.size == 2)
  }  

  /**
   * test composite store.
   * Splits keys into two parts using a Tuple2: first part is row-key whic is an HList, second is col-key which is an HList, too   
   */  
  def putAndGetCompositeStore(cluster: CassandraConfiguration.StoreCluster, keyspace: CassandraConfiguration.StoreKeyspace) = {
    import ScalaSerializables._
    import shapeless._
    import CassandraCompositeStore._
    
	val columnFamily = new CassandraConfiguration.StoreColumnFamily(comcf)
    val valser = implicitly[CassandraSerializable[String]]
  
    // create serializer-hlist by example
    val keys = "Testme" :: 56 :: 2.3d :: HNil
    val keySerializers = CassandraCompositeStore.getSerializerHListByExample(keys)
    // explicitly create serializer-hlist (alternative approach)
    val colSerializers = implicitly[CassandraSerializable[BigInteger]] :: implicitly[CassandraSerializable[String]] :: implicitly[CassandraSerializable[Int]] :: HNil
    val cols = new BigInteger("234") :: "123" :: 2 :: HNil 
    
    // sets up columnfamily
    CassandraCompositeStore.setupStore(cluster, keyspaceName, columnFamily, keySerializers, colSerializers, valser.getSerializer)
    
    // intialize store (don't forget to pull object CassandraCompositeStore into implicit scope)
    val store = new CassandraCompositeStore(keyspace, columnFamily, keySerializers, colSerializers, valser.getSerializer);

    val expected = Some("string-test")
    Await.result(store.put(((keys, cols), expected)))
    val result = Await.result(store.get((keys, cols)))
    
    // --- now test PartiallyIterableStore
    // some more columns... to have s.th. to query 
    val cols2 = new BigInteger("234") :: "123" :: 4 :: HNil 
    val cols3 = new BigInteger("234") :: "123" :: 7 :: HNil 
    val cols4 = new BigInteger("234") :: "123" :: 9 :: HNil 
    Await.result(store.put(((keys, cols2), expected)))    
    Await.result(store.put(((keys, cols3), expected)))
    Await.result(store.put(((keys, cols4), expected)))
    
    val start = (keys, cols)
    val end = Some((keys, new BigInteger("234") :: "123" :: 8 :: HNil ))
    // we have an implicit to order our keys; the ordering is not implied by Cassandra
    val result2 =  Await.result(store.multiGet(start, end))
    
    (result == expected) && (result2.size == 3)   
  }

  /**
   * test key-value-store
   */
  def putAndGetStoreWithTTL(cluster: CassandraConfiguration.StoreCluster, keyspace: CassandraConfiguration.StoreKeyspace) = {
    import ScalaSerializables._
    
    val columnFamily = new CassandraConfiguration.StoreColumnFamily(storecf)
    // sets up columnfamily, type param = type of keys
    CassandraStore.setupStore[BigInteger](cluster, keyspaceName, columnFamily)
    
    // set a short ttl, such that it can be tested the value is gone 
    val ttl = Some(Duration(5, TimeUnit.SECONDS))
    
    // intialize Store
    val store = CassandraStore[BigInteger, Long](keyspace, columnFamily, ttl = ttl)
    
    val expected = Some(1234L)
    val rowKey = new BigInteger("1234567890123456789")
    Await.result(store.put(rowKey, expected))
    var result = Await.result(store.get(rowKey)) == expected
    
    // check that "expected" is gone if waiting succeeds
    Try(Thread.sleep(ttl.get.inUnit(TimeUnit.MILLISECONDS))).onSuccess{
      _ => result = result && Await.result(store.get(rowKey)) == None
    }
    result
  }
  
  /**
   * merge is the only really secure operation for this store, so don't test get and put
   */
  def mergeLongStore(cluster: CassandraConfiguration.StoreCluster, keyspace: CassandraConfiguration.StoreKeyspace) = {
    import ScalaSerializables._
    
    val columnFamily = new CassandraConfiguration.StoreColumnFamily(countercf)
    // sets up columnfamily, type param = type of keys
    CassandraLongStore.setupStore[String](cluster, keyspaceName, columnFamily)
    
    // intialize Store
    val store = CassandraLongStore[String](keyspace, columnFamily)
    
    val value = 1234L
    val number = 4
    val expected = (number - 1) * value
    var result = 0L
    for(_ <- (1 to number)) result = Await.result(store.merge("test-string", value)).getOrElse(0L)
    result == expected
  }

  property("Various Cassandrastores put, get and merge") = {
    val host = new CassandraConfiguration.StoreHost(hostname)  
    val cluster = new CassandraConfiguration.StoreCluster(clusterName, host)
    val keyspace = new CassandraConfiguration.StoreKeyspace(keyspaceName, cluster)
    
    createKeyspace(cluster)
    try {
    	putAndGetStoreWithTTL(cluster, keyspace) &&
    		putAndGetDynamicComposite(cluster, keyspace) && 
    		putAndGetCompositeStore(cluster, keyspace) &&
    		mergeLongStore(cluster, keyspace)
    } finally {
    	removeOldStuff(cluster)
    }
  }

  def removeOldStuff(cluster: CassandraConfiguration.StoreCluster) = {
    cluster.getCluster.dropKeyspace(keyspaceName)    
  }
  
  def createKeyspace(cluster: CassandraConfiguration.StoreCluster) = {
    import scala.collection.JavaConversions._
    val ksDef = HFactory.createKeyspaceDefinition(keyspaceName,  ThriftKsDef.DEF_STRATEGY_CLASS, 1, Array[ColumnFamilyDefinition]().toList)
    cluster.getCluster.addKeyspace(ksDef, true)
  }
  


}

