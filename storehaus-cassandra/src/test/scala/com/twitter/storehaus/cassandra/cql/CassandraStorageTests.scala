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
package com.twitter.storehaus.cassandra.cql

import com.twitter.algebird.Semigroup
import com.twitter.util.{ Await, Duration, Try }
import com.websudos.phantom.CassandraPrimitive
import org.scalacheck.{ Gen, Properties }
import org.scalacheck.Prop.forAll
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit
import shapeless._

/** Unit tests for Cassandra */
object CassandraStoreProperties extends Properties("CassandraStore") {
  val hostname = "localhost"
  val clusterName = "Test Cluster"
  val keyspaceName = "Myspace"
  val countercf = "countercf"
  val collcf = "collectioncompositecf"
  val comcf = "compositecf"
  val storecf = "kvstorecf"

  /**
   * test basic key-value-store
   */
  def putAndGetBasicKeyValeStore(session: CQLCassandraConfiguration.StoreSession) = {
    
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(storecf, session)
    
    // sets up columnfamily, type param = type of keys
    CQLCassandraStore.createColumnFamily[BigInt, Date](columnFamily)
    
    // intialize Store
    val store = new CQLCassandraStore[BigInt, Date](columnFamily)
    
    // create value and key
    val expected = Some(new Date)
    val rowKey = BigInt("1234567890123456789")
    
    // put and get value
    Await.result(store.put(rowKey, expected))
    val result = Await.result(store.get(rowKey)) == expected
    
    // delete value
    Await.result(store.put(rowKey, None))
    val result2 = Await.result(store.get(rowKey)) == None

    if(!(result && result2)) println("Cassandra-Results do not match in putAndGetBasicKeyValeStore")
    result && result2
  }
      
  /**
   * test mergeable collection store
   * a key is a Tuple2: first part is row-key, second col-key  
   */
  def putAndGetAndMergeCollectionComposite(session: CQLCassandraConfiguration.StoreSession) = {
	import AbstractCQLCassandraCompositeStore._
    
	val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(collcf, session)

	// partition- and column keys
    val rk = "TestrowPart1" :: 234 :: HNil
	val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
	val key = (rk, ck)
	  
	// names of partitions and columns
	val rowNames = List("rkKey1", "rkKey2")
	val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
	
	// create serializer list from keys
	val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
	val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
	
	// pull value serializer out of implicit scope
	val valueSerializer = implicitly[CassandraPrimitive[String]]
    val expected = Some(Set("test-string"))
	
	// sets up columnfamily, uses a Set to store values
	CQLCassandraCollectionStore.createColumnFamily(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializer, Set[String]())
    
	// intialize store
	val store = new CQLCassandraCollectionStore(columnFamily, rkSerializers, rowNames, ckSerializers, colNames)(implicitly[Semigroup[Set[String]]])
    
	// put and get value
    Await.result(store.put(key, expected))
    val result = Await.result(store.get(key))
    
    // merge in value
    Await.result(store.merge((key, Set("test-string2"))))
    // Thread.sleep(20000)
    val result2 = Await.result(store.get(key))
    
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
    
    if(!(result == expected)) println("Cassandra-Results do not match in putAndGetAndMergeCollectionComposite, expected was: " + expected + " but result was " + result)
    if(result2 == None || result2.get.size != 2) println("Cassandra-Results do not match in putAndGetAndMergeCollectionComposite, expected merge to return two elements in set, but set was:" + result2)
    (result == expected) && (result2 != None && result2.get.size == 2)
  }  

  /**
   * test composite store and ttl
   */
  def putAndGetCompositeStoreWithTTL(session: CQLCassandraConfiguration.StoreSession) = {
	import AbstractCQLCassandraCompositeStore._
    
	val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(comcf, session)

	// partition- and column keys
    val rk = "TestrowPart1" :: 234 :: HNil
	val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
	val key = (rk, ck)
	  
	// names of partitions and columns
	val rowNames = List("rkKey1", "rkKey2")
	val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
	
	// create serializer list from keys
	val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
	val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
	
	// pull value serializer out of implicit scope
	val valueSerializer = implicitly[CassandraPrimitive[UUID]]
    val expected = Some(UUID.randomUUID())
	
	// sets up columnfamily, uses a Set to store values
	CQLCassandraCompositeStore.createColumnFamily(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializer)
    
	// intialize store with ttl
	val ttl = Some(Duration(5, TimeUnit.SECONDS))
	val store = new CQLCassandraCompositeStore(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, ttl = ttl)(valueSerializer)
    
	// put and get value
    Await.result(store.put(key, expected))
    var result = Await.result(store.get(key)) == expected

    if(!(result)) println("Cassandra-Results do not match in putAndGetCompositeStoreWithTTL")

    // wait until value is gone (i.e. ttl is exceeded)
    Try(Thread.sleep(ttl.get.inUnit(TimeUnit.MILLISECONDS))).onSuccess{
      _ => result = result && Await.result(store.get(key)) == None
    }
	
    if(!result) println("Cassandra did not delete value after TTL in putAndGetCompositeStoreWithTTL")
	
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
	
    result
  }

  /**
   * mergeable store with Cassandra's counters
   */
  def mergeLongStore(session: CQLCassandraConfiguration.StoreSession) = {
    import AbstractCQLCassandraCompositeStore._
    
    val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(countercf, session)

  	// partition- and column keys
    val rk = "TestrowPart1" :: 234 :: HNil
	val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
	val key = (rk, ck)
	  
	// names of partitions and columns
	val rowNames = List("rkKey1", "rkKey2")
	val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
	
	// create serializer list from keys
	val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
	val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)

    // sets up columnfamily, type param = type of keys
    CQLCassandraLongStore.createColumnFamily(columnFamily, rkSerializers, rowNames, ckSerializers, colNames)
    
    // intialize Store
    val store = new CQLCassandraLongStore(columnFamily, rkSerializers, rowNames, ckSerializers, colNames)()
    
    // test merge
    val value = 1234L
    val number = 4
    val expected = (number - 1) * value

    var result = 0L
    for(_ <- (1 to number)) result = Await.result(store.merge(key, value)).getOrElse(0L)
    
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
    
    result == expected
  }


  property("Various Cassandrastores put, get and merge") = {
    val host = new CQLCassandraConfiguration.StoreHost(hostname)
    val cluster = new CQLCassandraConfiguration.StoreCluster(clusterName, Set(host))
    val session = new CQLCassandraConfiguration.StoreSession(keyspaceName, cluster)
    
    println("Creating Cassandra-Keyspace " + keyspaceName)
    session.createKeyspace
    try {
    	println("Executing Cassandra-Tests")
        val r1 = putAndGetBasicKeyValeStore(session)
    	val r2 = putAndGetAndMergeCollectionComposite(session) 
    	val r3 = putAndGetCompositeStoreWithTTL(session) 
    	val r4 = mergeLongStore(session)
    	println("Execution of Cassandra-Tests finished with success=" + (r1 && r2 && r3 && r4))
        
    	r1 && r2 && r3 && r4
    } finally {
    	session.dropAndDeleteKeyspaceAndContainedData
    	cluster.getCluster.close
    	println("Deleted Cassandra-Keyspace " + keyspaceName)
    }
  }
}