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
import com.datastax.driver.core.ProtocolVersion
import org.scalacheck.{ Gen, Properties }
import org.scalacheck.Prop.forAll
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit
import shapeless._
import HList._
import ops.hlist.Mapper
import ops.hlist.Mapped
import ops.hlist.ToList
import ops.hlist.Tupler
import shapeless.syntax.std.tuple._
import Nat._
import UnaryTCConstraint._
import scala.annotation.implicitNotFound
import com.twitter.storehaus.cassandra.cql.macrobug._


/** Unit tests for Cassandra */
object CassandraStoreProperties extends Properties("CassandraStore") {
  val hostname = "localhost"
  val clusterName = "Test Cluster"
  val keyspaceName = "Myspace"
  val countercf = "countercf"
  val collcf = "collectioncompositecf"
  val comcf = "compositecf"
  val storecf = "kvstorecf"
  val storevaluescf = "kmultivcf"
  val tuplevaluescf = "tuplecf"
  val rowvaluescf = "rowcf"

  /**
   * test basic key-value-store
   */
  def putAndGetBasicKeyValueStore(session: CQLCassandraConfiguration.StoreSession) = {
    
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(storecf, session)
    
    // sets up columnfamily, type param = type of keys
    CQLCassandraStore.createColumnFamily[BigInt, Date](columnFamily)
    
    // intialize Store
    val store = new CQLCassandraStore[BigInt, Date](columnFamily)
    
    // create value and key
    val expected = Some(new Date)
    val rowKey = BigInt("1234567890123456789")
    
    // put and get value
    Await.result(store.put((rowKey, expected)))
    val result = Await.result(store.get(rowKey)) == expected
    
    // delete value
    Await.result(store.put((rowKey, None)))
    val result2 = Await.result(store.get(rowKey)) == None

    if(!(result && result2)) println("Cassandra-Results do not match in putAndGetBasicKeyValeStore")
    result && result2
  }
  
  /**
   * test basic key-value-store with scala tuples as values
   */
  def putAndGetBasicKeyMultiValueStore(session: CQLCassandraConfiguration.StoreSession) = {
    import AbstractCQLCassandraCompositeStore._
    
    // define types for the store values as HList first 
    type ValueHListType = Option[Date] :: Option[String] :: HNil
    // same type as value type using tuple (which is used for put and get)
    type ValueType = (Option[Date], Option[String])
    // similarly define serializer types, which are always CassandraPrimitives
    type SerializerType = CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil
    
    // create column family instance to work on
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(storevaluescf, session)
    // decide about column names
    val valueColumnNames = List("Birthdate", "Name")
    
    // create value and key
    val expectedashlist = new Date :: "John Doe" :: HNil
    val expected = Some((Some(expectedashlist.head), Some(expectedashlist.tail.head)))
    val rowKey = BigInt("1234567890123456789")
    
    // create serializers by eymaple (doesn't need to be of exact values, of course, but types must fit), 
    // can be done if no special types are in use, needs import
    val valueSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(expectedashlist)

    // sets up columnfamily, type param = type of keys
    CQLCassandraStoreTupleValues.createColumnFamily[BigInt, CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil,
      String :: String :: HNil](columnFamily, valueColumnNames, valueSerializers)
    
    // intialize Store
    val store = new CQLCassandraStoreTupleValues[BigInt, (Option[Date], Option[String]), Option[Date] :: Option[String] :: HNil, 
      CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil](columnFamily, valueColumnNames, valueSerializers)
    
    // put and get value
    Await.result(store.put((rowKey, expected)))
    var result = Await.result(store.get(rowKey)) == expected
    
    // test IterableStore (this is a very simple test, as only one item can be returned)
    val head = Await.result(store.getAll)
    result = result && Await.result(head.tail).isEmpty 
    
    if(!result) println("Cassandra-Results do not match in putAndGetBasicKeyValeStore")
    result
  }

  /**
   * test basic key-value-store with Cassandra-rows as values
   */
  def putAndGetKeyRowStore(session: CQLCassandraConfiguration.StoreSession) = {
    
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(rowvaluescf, session)
    
    val keycolumn = "somerowkey"
    // define columns the rows will have
    val columnsOfRow = List(("somerowkey", implicitly[CassandraPrimitive[UUID]]),
        ("firstname", implicitly[CassandraPrimitive[String]]),
        ("surname", implicitly[CassandraPrimitive[String]]),
        ("zip", implicitly[CassandraPrimitive[Int]]),
        ("city", implicitly[CassandraPrimitive[String]]),
        ("personid", implicitly[CassandraPrimitive[UUID]]),
        ("female", implicitly[CassandraPrimitive[Boolean]]),
        ("birthdate", implicitly[CassandraPrimitive[Date]]),
        ("account", implicitly[CassandraPrimitive[BigDecimal]]))
    
    // sets up columnfamily, type param = type of keys
    CQLCassandraRowStore.createColumnFamily[UUID](columnFamily, columnsOfRow, keycolumn)

    // intialize Store
    val store = new CQLCassandraRowStore[UUID](columnFamily, columnsOfRow, keycolumn)
    
    // create value and key
    val zip = 9585478
    val city = "Gossipham"
    val rowKey = UUID.randomUUID()
    val value = Some(new CQLCassandraRow(Map("somerowkey" -> rowKey, "firstname" -> "Bad", "surname" -> "Man",
        "zip" -> zip, "city" -> city, "personid" -> UUID.randomUUID(), "female" -> false, 
        "birthdate" -> new Date(), "account" -> BigDecimal.double2bigDecimal(Math.random()))))
    
    // put and get value
    Await.result(store.put((rowKey, value)))
    val resultrow = Await.result(store.get(rowKey))
    val result = resultrow.get.getString("city") == city && resultrow.get.getInt("zip") == zip
    
    // delete value
    Await.result(store.put((rowKey, None)))
    val result2 = Await.result(store.get(rowKey)) == None

    if(!(result && result2)) println("Cassandra-Results do not match in putAndGetKeyRowStore")
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
    
	// intialize store; due to some shapeless change type inference doesn't seem to work any more
	val store = new CQLCassandraCollectionStore[String :: Int :: HNil, String :: Double :: UUID :: HNil, Set[String], String, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil](
	      columnFamily, rkSerializers, rowNames, ckSerializers, colNames)(implicitly[Semigroup[Set[String]]])
    
	// put and get value
    Await.result(store.put((key, expected)))
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
   * test composite store and ttl, as well as TupleStore, which is a tupled wrapper for composite store
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
	val store = new CQLCassandraCompositeStore[String :: Int :: HNil, String :: Double :: UUID :: HNil, UUID, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil](
	    columnFamily, rkSerializers, rowNames, ckSerializers, colNames, ttl = ttl)(valueSerializer)
    
	// put and get value
    Await.result(store.put((key, expected)))
    var result = Await.result(store.get(key)) == expected

    if(!(result)) println("Cassandra-Results do not match in putAndGetCompositeStoreWithTTL")

    // wait until value is gone (i.e. ttl is exceeded)
    Try(Thread.sleep(ttl.get.inUnit(TimeUnit.MILLISECONDS))).onSuccess{
      _ => result = result && Await.result(store.get(key)) == None
    }
	
    if(!result) println("Cassandra did not delete value after TTL in putAndGetCompositeStoreWithTTL")
	
    // do s.th. with CassandraTupleStore to test that we can also use simple Tuples instead of HLists
    val tuplestore = new CassandraTupleStore(store, (("", 0), ("", 0.0d, UUID.randomUUID())))
    val newkey = (("1", 1), ("1", 1.0d, UUID.randomUUID()))
    Await.result(tuplestore.put((newkey, expected)))
    result = result && Await.result(tuplestore.get(newkey)) == expected

    if(!(result)) println("Cassandra-Results do not match in putAndGetCompositeStoreWithTTL for CassandraTupleStore")
    
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
	
    result
  }

  /**
   * test multi-value store as well as a tupled wrapper for it
   */
  def putAndGetCompositeStoreWithTupleValues(session: CQLCassandraConfiguration.StoreSession) = {
	import AbstractCQLCassandraCompositeStore._
    
	val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(tuplevaluescf, session)

	// partition- and column keys
    val rk = "TestrowPart1" :: 234 :: HNil
	val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
	val key = (rk, ck)
	  
	// names of partitions and columns
	val rowNames = List("rkKey1", "rkKey2")
	val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
	val valNames = List("valCol1", "whatever", "there")
	
	// create serializer list from keys
	val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
	val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
	
	// pull value serializer out of implicit scope
	val valueSerializers = implicitly[CassandraPrimitive[UUID]] :: implicitly[CassandraPrimitive[String]] :: implicitly[CassandraPrimitive[Double]] :: HNil
    val expected = Some(UUID.randomUUID() :: "man" :: 1.2d :: HNil)
	
	// sets up columnfamily, uses a Set to store values
	CQLCassandraMultivalueStore.createColumnFamily(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializers, valNames)
    
	// intialize store
	val store = new CQLCassandraMultivalueStore[String :: Int :: HNil, String :: Double :: UUID :: HNil, UUID :: String :: Double :: HNil, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil,
	  CassandraPrimitive[UUID] :: CassandraPrimitive[String] :: CassandraPrimitive[Double] :: HNil](
	    columnFamily, rkSerializers, rowNames, ckSerializers, colNames)(valueSerializers, valNames)
    
	// put and get value
    Await.result(store.put((key, expected)))
    var result = Await.result(store.get(key)) == expected

    if(!(result)) println("Cassandra-Results do not match in putAndGetCompositeStoreWithTupleValues")

    // do s.th. with CassandraTupleStore to test that we can also use simple Tuples instead of HLists
    val tuplestore = new CassandraTupleMultiValueStore(store, (("", 0), ("", 0.0d, UUID.randomUUID())), (UUID.randomUUID, "", 0.0d))
    val newexpected = Some((UUID.randomUUID(), "man", 1.2d))
	val newkey = (("1", 1), ("1", 1.0d, UUID.randomUUID()))
    Await.result(tuplestore.put((newkey, newexpected)))
    result = result && Await.result(tuplestore.get(newkey)) == newexpected

    if(!(result)) println("Cassandra-Results do not match in putAndGetCompositeStoreWithTupleValues for CassandraTupleMultiValueStore")
    
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
    val store = new CQLCassandraLongStore[String :: Int :: HNil, String :: Double :: UUID :: HNil,
	  CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
	  CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil](
	      columnFamily, rkSerializers, rowNames, ckSerializers, colNames)()
	      
    // test merge
    val value = 1234L
    val number = 4
    val expected = (number - 1) * value

    var result = 0L
    for(_ <- (1 to number)) result = Await.result(store.merge((key, value))).getOrElse(0L)
    
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
    
    result == expected
  }


  property("Various Cassandrastores put, get and merge") = {
    val host = new CQLCassandraConfiguration.StoreHost(hostname)
    val cluster = new CQLCassandraConfiguration.StoreCluster(clusterName, Set(host), protocolVersion = ProtocolVersion.V2)
    val session = new CQLCassandraConfiguration.StoreSession(keyspaceName, cluster, 
        "{'class' : 'SimpleStrategy', 'replication_factor' : 1}")
    
    session.createKeyspace
    try {
        val r1 = putAndGetBasicKeyValueStore(session)
    	val r2 = putAndGetAndMergeCollectionComposite(session) 
    	val r3 = putAndGetCompositeStoreWithTTL(session) 
    	val r4 = mergeLongStore(session)
        val r5 = putAndGetCompositeStoreWithTupleValues(session)
        val r6 = putAndGetBasicKeyMultiValueStore(session)
        val r7 = putAndGetKeyRowStore(session)
    	
    	r1 && r2 && r3 && r4 && r5 && r6 && r7
    } finally {
    	// session.dropAndDeleteKeyspaceAndContainedData
    	cluster.getCluster.close
    }
  }
}