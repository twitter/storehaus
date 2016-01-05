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
import com.twitter.util.{Await, Duration, Try}
import com.websudos.phantom.CassandraPrimitive
import com.datastax.driver.core.ProtocolVersion
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit
import shapeless._
import shapeless.HList._
import shapeless.ops.hlist.{Mapped, Mapper, ToList, Tupler}
import shapeless.ops.product.ToHList
import shapeless.syntax.std.tuple._
import scala.annotation.implicitNotFound
import AbstractCQLCassandraCompositeStore._
import scala.collection.mutable.ArrayBuffer
import com.datastax.driver.core.querybuilder.Clause
import scala.util.Random

/** Tests for Storehaus-Cassandra */
class CassandraStoreProperties extends FlatSpec with BeforeAndAfterAll {
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

  val host = new CQLCassandraConfiguration.StoreHost(hostname)
  lazy val cluster = new CQLCassandraConfiguration.StoreCluster(clusterName, Set(host), protocolVersion = ProtocolVersion.V2)
  lazy val session = new CQLCassandraConfiguration.StoreSession(keyspaceName, cluster, 
        "{'class' : 'SimpleStrategy', 'replication_factor' : 1}")
  
  override def beforeAll() {
    session.createKeyspace
  }
  
  override def afterAll() {
    // session.dropAndDeleteKeyspaceAndContainedData
    cluster.getCluster.close
  }
  
  /**
   * these are the same for all stores, all types are fixed, so we can generalize without getting
   * type parameter extremeness
   */
  def checkCasStoreOperations[K, V](casstore: CASStore[Long, K, V], key: K, expected: V) = {
    // cas the first value ensuring that it did not previously exist by providing None
    val firstresult = Await.result(casstore.cas(None, (key, expected)))
    assert(firstresult)
    
    // cas the second value ensuring that it did not previously exist => this should fail
    val secondresult = Await.result(casstore.cas(None, (key, expected)))
    assert(secondresult === false)
    
    // fetch the current token, so we can use the token as the value to be checked
    val token = Await.result(casstore.get(key)).get._2
    
    // cas a value with the correct token, cas-sing with the right token should produce true
    val thirdresult = Await.result(casstore.cas(Some(token), (key, expected)))
    assert(thirdresult)

    // fetch new version for testing for it later on
    val newToken = Await.result(casstore.get(key)).get._2
    
    // now the version has advanced and the token should be incorrect; token should still be newToken
    val forthresult = Await.result(casstore.casAndGet(Some(token), (key, expected)))
    // * cas-sing with an aged version token should have produced false
    assert(forthresult._1 === false)
    // * returned token should be identical to the newToken previously returned from get
    assert(newToken === forthresult._2.get._2)
  }
  
  behavior of "Basic Cassandra Key-Value-Store"
  
  it should "put a value and get as well as delete it" in {
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
    assert(Await.result(store.get(rowKey)) === expected)
    
    // delete value
    Await.result(store.put((rowKey, None)))
    assert(Await.result(store.get(rowKey)) === None)    
  }
  
  it should "be able to produce a cas-store and do various cas operations" in {
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(storecf + "cas", session)
    CQLCassandraStore.createColumnFamilyWithToken[BigInt, Date, Long](columnFamily, Some(implicitly[CassandraPrimitive[Long]]))
    val store = new CQLCassandraStore[BigInt, Date](columnFamily)
    
    // create the cas store from the original one
    val casstore = store.getCASStore[Long]()
    val expected = new Date
    val rowKey = BigInt(Random.nextLong())
    
    checkCasStoreOperations(casstore, rowKey, expected)
  }
  
  behavior of "Basic Cassandra Key-Value-Store with tuples as values"
  
  it should "put a value and get as well test IterableStore capabilities" in {    
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
    
    // create serializers by example (doesn't need to be of exact values, of course, but types must fit), 
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
    assert(Await.result(store.get(rowKey)) === expected)
    
    // test IterableStore (this is a very simple test, as only one item can be returned)
    val head = Await.result(store.getAll)
    assert(Await.result(head.tail).isEmpty)
  }

  it should "be able to produce a cas-store and do various cas operations" in {
    type ValueHListType = Option[Date] :: Option[String] :: HNil
    type ValueType = (Option[Date], Option[String])
    type SerializerType = CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(storevaluescf + "cas", session)
    val valueColumnNames = List("Birthdate", "Name")
    val expectedashlist = new Date :: "John Doe" :: HNil
    val expected = (Some(expectedashlist.head), Some(expectedashlist.tail.head))
    val rowKey = BigInt(Random.nextLong())
    val valueSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(expectedashlist)

    // sets up columnfamily, type param = type of keys
    CQLCassandraStoreTupleValues.createColumnFamilyWithToken[BigInt, CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil,
      String :: String :: HNil, Long](columnFamily, valueColumnNames, valueSerializers, Some(implicitly[CassandraPrimitive[Long]]))
    
    // intialize Store
    val store = new CQLCassandraStoreTupleValues[BigInt, (Option[Date], Option[String]), Option[Date] :: Option[String] :: HNil, 
      CassandraPrimitive[Date] :: CassandraPrimitive[String] :: HNil](columnFamily, valueColumnNames, valueSerializers)

    // create the cas store from the original one
    val casstore = store.getCASStore[Long]()
    
    checkCasStoreOperations(casstore, rowKey, expected)
  }
  
  behavior of "Cassandra Key-Row-Store"

  it should "put a row and get as well as delete it" in {
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
    assert(resultrow.get.getString("city") === city)
    assert(resultrow.get.getInt("zip") === zip)
    
    // delete value
    Await.result(store.put((rowKey, None)))
    assert(Await.result(store.get(rowKey)) === None)
  }  
  
  it should "be able to produce a cas-store and do various cas operations" in {
    val columnFamily = CQLCassandraConfiguration.StoreColumnFamily(rowvaluescf + "cas", session)
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
    CQLCassandraRowStore.createColumnFamilyWithToken[UUID, Long](columnFamily, 
        Some(implicitly[CassandraPrimitive[Long]]), columnsOfRow, keyColumnName = keycolumn)

    // intialize Store
    val store = new CQLCassandraRowStore[UUID](columnFamily, columnsOfRow, keycolumn)
    
    // create value and key
    val zip = 9585478
    val city = "Gossipham"
    val rowKey = UUID.randomUUID()
    val expected = new CQLCassandraRow(Map("somerowkey" -> rowKey, "firstname" -> "Bad", "surname" -> "Man",
        "zip" -> zip, "city" -> city, "personid" -> UUID.randomUUID(), "female" -> false, 
        "birthdate" -> new Date(), "account" -> BigDecimal.double2bigDecimal(Math.random())))
    
    // create the cas store from the original one
    val casstore = store.getCASStore[Long]()
    
    checkCasStoreOperations(casstore, rowKey, expected)
  }
  
  behavior of "Composite Cassandra Key-Value-Store with collections as values"
  
  /**
   * test mergeable collection store
   * a key is a Tuple2: first part is row-key, second col-key  
   */
  it should "put a value (collection) and get as well as merge with another one" in {
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
    val result2 = Await.result(store.get(key))
    
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
    
    assert(result === expected) 
    assert(result2 != None)
    assert(result2.get.size === 2)
  }  

  it should "be able to produce a cas-store and do various cas operations" in {
    val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(collcf + "cas", session)
    val rk = "TestrowPart1" :: 234 :: HNil
    val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
    val key = (rk, ck)
    val rowNames = List("rkKey1", "rkKey2")
    val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
    val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
    val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
    val valueSerializer = implicitly[CassandraPrimitive[String]]
    val expected = Set("test-string")
    
    // create column family with token information
    CQLCassandraCollectionStore.createColumnFamilyWithToken(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializer, Set[String](), Some(implicitly[CassandraPrimitive[Long]]))  
    val store = new CQLCassandraCollectionStore[String :: Int :: HNil, String :: Double :: UUID :: HNil, Set[String], String, 
      CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
      CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil](
          columnFamily, rkSerializers, rowNames, ckSerializers, colNames)(implicitly[Semigroup[Set[String]]])
    // create cas-store from collection store
    val casstore = store.getCASStore[Long]()
    
    checkCasStoreOperations(casstore, key, expected)
  }

  behavior of "Composite Cassandra Key-Value-Store with simple values"
  
  def setupCompositeStore[RK <: HList, CK <: HList, RS <: HList, CS <: HList](rk: RK, ck: CK, rkSerializers: RS, 
      ckSerializers: CS, setup: Boolean = false, ttl: Option[Duration] = None, withcas: Boolean = false)(
          implicit strrs: CassandraPrimitivesToStringlist[RS],
          strcs: CassandraPrimitivesToStringlist[CS],
          evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
          evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
          rowmap: Row2Result.Aux[RS, RK],
          colmap: Row2Result.Aux[CS, CK],
          a2rk: Append2Composite[ArrayBuffer[Clause], RK, RS],
          a2ck: Append2Composite[ArrayBuffer[Clause], CK, CS]) = {

    val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(comcf + (if(withcas) "cas" else ""), session)
    // names of partitions and columns
    val rowNames = List("rkKey1", "rkKey2")
    val colNames = List("ckKey1", "someOtherNamesOfYourChoice", "aUUIDCol")
    
    // pull value serializer out of implicit scope
    val valueSerializer = implicitly[CassandraPrimitive[UUID]]
       
    // sets up columnfamily, uses a Set to store values
    if(setup) {
      if(withcas) {
        CQLCassandraCompositeStore.createColumnFamilyWithToken(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializer, Some(implicitly[CassandraPrimitive[Long]]))
      } else {
        CQLCassandraCompositeStore.createColumnFamily(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializer)
      }
    }

    // intialize store
    new CQLCassandraCompositeStore[RK, CK, UUID, RS, CS](
      columnFamily, rkSerializers, rowNames, ckSerializers, colNames, ttl = ttl)(valueSerializer)
  }
  
  /**
   * test composite store and ttl, as well as TupleStore, which is a tupled wrapper for composite store
   */
  it should "put and get" in {
  	// partition- and column keys
    val rk = "TestrowPart1" :: 234 :: HNil
  	val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
  	val key = (rk, ck)
  	  
    val expected = Some(UUID.randomUUID())

    // create serializer list from keys
    val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
    val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
    
    val store = setupCompositeStore(rk, ck, rkSerializers, ckSerializers, true)
    
	  // put and get value
    Await.result(store.put((key, expected)))
    assert(Await.result(store.get(key)) === expected)
  }

  it should "be able to be used with CassandraTupleStore" in {
    val rk = "TestrowPart1" :: 234 :: HNil
    val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
    val expected = Some(UUID.randomUUID())
    val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
    val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
    val store = setupCompositeStore(rk, ck, rkSerializers, ckSerializers)
    
    // setup tuple store
    val tuplestore = new CassandraTupleStore(store, (("", 0), ("", 0.0d, UUID.randomUUID())))
    val newkey = (("1", 1), ("1", 1.0d, UUID.randomUUID()))
    Await.result(tuplestore.put((newkey, expected)))
    assert(Await.result(tuplestore.get(newkey)) === expected)
    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
  }
  
  it should "put and adhere to ttl constraints" in {
    val rk = "TestrowPart11" :: 234 :: HNil
    val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
    val key = (rk, ck)
    val expected = Some(UUID.randomUUID())
    val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
    val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
    // setup store with ttl
    val ttl = Some(Duration(5, TimeUnit.SECONDS))
    val store = setupCompositeStore(rk, ck, rkSerializers, ckSerializers, ttl = ttl)
    
    // put and get value
    Await.result(store.put((key, expected)))
    assert(Await.result(store.get(key)) === expected)
    // wait until value is gone (i.e. ttl is exceeded)
    Try(Thread.sleep(ttl.get.inUnit(TimeUnit.MILLISECONDS))).onSuccess{
      _ => assert(Await.result(store.get(key)) === None)
    }
  }
  
  it should "be able to produce a cas-store and do various cas operations" in {
    val rk = "TestrowPart1" :: 234 :: HNil
    val ck = "ColPart1" :: 2.34d :: UUID.randomUUID() :: HNil
    val key = (rk, ck)
    val expected = UUID.randomUUID()
    val rkSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(rk)
    val ckSerializers = AbstractCQLCassandraCompositeStore.getSerializerHListByExample(ck)
    val store = setupCompositeStore(rk, ck, rkSerializers, ckSerializers, true, withcas = true)

    // create cas-store from store
    val casstore = store.getCASStore[Long]()
    
    checkCasStoreOperations(casstore, key, expected)
  }
  
  behavior of "Composite Cassandra Key-Value-Store with Tuple values"
  
  it should "put and get, and use store as CassandraTupleStore" in {
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
    assert(Await.result(store.get(key)) === expected)
 
    // do s.th. with CassandraTupleStore to test that we can also use simple Tuples instead of HLists
    val tuplestore = new CassandraTupleMultiValueStore(store, (("", 0), ("", 0.0d, UUID.randomUUID())), (UUID.randomUUID, "", 0.0d))
    val newexpected = Some((UUID.randomUUID(), "man", 1.2d))
    val newkey = (("1", 1), ("1", 1.0d, UUID.randomUUID()))
    Await.result(tuplestore.put((newkey, newexpected)))
    assert(Await.result(tuplestore.get(newkey)) === newexpected)

    // close store to shutdown futurePool correctly
    store.close(Duration(10, TimeUnit.SECONDS))
  }
  
  it should "be able to produce a cas-store and do various cas operations" in {
    val columnFamily = new CQLCassandraConfiguration.StoreColumnFamily(tuplevaluescf + "cas", session)
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
    val expected = UUID.randomUUID() :: "man" :: 1.2d :: HNil
   
    // sets up columnfamily, uses a Set to store values
    CQLCassandraMultivalueStore.createColumnFamilyWithToken(columnFamily, rkSerializers, rowNames, ckSerializers, colNames, valueSerializers, valNames, Some(implicitly[CassandraPrimitive[Long]]))
     
    // intialize store
    val store = new CQLCassandraMultivalueStore[String :: Int :: HNil, String :: Double :: UUID :: HNil, UUID :: String :: Double :: HNil, 
      CassandraPrimitive[String] :: CassandraPrimitive[Int] :: HNil, 
      CassandraPrimitive[String] :: CassandraPrimitive[Double] :: CassandraPrimitive[UUID] :: HNil,
      CassandraPrimitive[UUID] :: CassandraPrimitive[String] :: CassandraPrimitive[Double] :: HNil](
        columnFamily, rkSerializers, rowNames, ckSerializers, colNames)(valueSerializers, valNames)

    
    // create cas-store from store
    val casstore = store.getCASStore[Long]()
    
    checkCasStoreOperations(casstore, key, expected)
  }
  
  behavior of "Composite Cassandra Key-Value-Store with Long values"
  /**
   * mergeable store with Cassandra's counters
   */
  it should "put a Long and get as well as merge with another one" in {
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
    
    // initialize Store
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
    
    assert(result === expected)
  }
}

