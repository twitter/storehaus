///*
// * Copyright 2014 SEEBURGER AG
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may
// * not use this file except in compliance with the License. You may obtain
// * a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.twitter.storehaus.cassandra
//
//import java.util.concurrent.Executors
//import com.twitter.util.{ Future, FuturePool }
//import com.twitter.storehaus.Store
//import me.prettyprint.hector.api.factory.HFactory
//import me.prettyprint.hector.api.{ Cluster, Keyspace, HConsistencyLevel, ConsistencyLevelPolicy, Serializer }
//import me.prettyprint.hector.api.beans.HColumn
//import me.prettyprint.hector.api.ddl.{ ComparatorType, KeyspaceDefinition }
//import me.prettyprint.hector.api.exceptions.HNotFoundException
//import me.prettyprint.hector.api.beans.DynamicComposite
//import me.prettyprint.cassandra.model.{ QuorumAllConsistencyLevelPolicy, ConfigurableConsistencyLevel }
//import me.prettyprint.cassandra.serializers.{ DynamicCompositeSerializer, StringSerializer }
//import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, ThriftKsDef }
//import scala.collection.JavaConversions._
//import ScalaSerializables._
//import com.twitter.util.Duration
//
///**
// *  A Cassandra wrapper using dynamic composite keys
// *
// *  The Store variant provided here has the ability to do slice queries over column slices, which allows
// *  for better performance and unknown column names (i.e. to store values in column names).
// *
// *  For convenience type parameters are more flexible than needed. One can pass in a bunch of
// *  Cassandra/Hector-serializbale objects and the corresponding serializers:
// *  	RK: represents the type(s) of the Cassandra-composite-row-key.
// *   		One can either pass in a single type or a List of types.
// *     		Remember it might not possible to peform range scans with random-partitioners like Murmur3.
// *   	CK: represents the type(s) of the Cassandra-composite-column-key.
// *    		See RK for details.
// *      	It is possible to perform queries on column slices.
// *       V: The type of the value.
// *
// *  @author Andreas Petter
// */
//
//object CassandraDynamicCompositeStore {
//
//  def apply[RK, CK, V: CassandraSerializable](
//    keyspace: StoreKeyspace,
//    columnFamily: StoreColumnFamily,
//    rowKeySerializers: List[CassandraSerializable],
//    columnKeySerializers: List[CassandraSerializable],
//    policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
//    poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
//    ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION): CassandraDynamicCompositeStore[RK, CK, V] =
//    new CassandraDynamicCompositeStore[RK, CK, V](keyspace, columnFamily, valueSerializer, policy, poolSize, ttl)
//
//  /**
//   * Optionally this method can be used to setup storage on the Cassandra cluster.
//   * (shouldn't work on on existing keyspaces and column-families)
//   */
//  def setupStore[RK, CK, V: CassandraSerializable](cluster: StoreCluster, keyspaceName: String, columnFamily: StoreColumnFamily, valueSerializer: Serializer[V], replicationFactor: Int) = {
//    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, ComparatorType.DYNAMICCOMPOSITETYPE);
//    cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
//    cfDef.setKeyValidationClass(ComparatorType.DYNAMICCOMPOSITETYPE.getClassName())
//    cfDef.setKeyValidationAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
//    cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getClassName())
//    val keyspace: KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
//      Array(cfDef).toList)
//    cluster.getCluster.addKeyspace(keyspace, true)
//  }
//}
//
//class CassandraDynamicCompositeStore[RK, CK, V: CassandraSerializable](
//  val keyspace: StoreKeyspace,
//  val columnFamily: StoreColumnFamily,
//  val rowKeySerializers: List[CassandraSerializable[Any]],
//  val columnKeySerializers: List[CassandraSerializable[Any]],
//  val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
//  val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
//  val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION)
//  extends Store[(RK, CK), V] {
//  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)
//  val rowkeySerializer = implicitly[CassandraSerializable[RK]]
//  val columnkeySerializer = implicitly[CassandraSerializable[CK]]
//  val valueSerializer = implicitly[CassandraSerializable[V]]
//
//  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))
//
//  override def withPutTtl(ttl: Duration): CassandraDynamicCompositeStore[RK, CK, V] = CassandraDynamicCompositeStore(keyspace,
//    columnFamily, policy, poolSize, Option(ttl))
//
//  override def put(kv: ((RK, CK), Option[V])): Future[Unit] = {
//    // create the keys for rows and columns
//    val ((rk, ck), valueOpt) = kv
//    val rowKey = createDynamicKey(rk, rowkeySerializer)
//    val colKey = createDynamicKey(ck, columnkeySerializer)
//
//    valueOpt match {
//      case Some(value) => futurePool {
//        // write the new entry to Cassandra
//        val mutator = HFactory.createMutator(keyspace, DynamicCompositeSerializer.get)
//        mutator.addInsertion(rowKey, columnFamilyName, HFactory.createColumn(colKey, value))
//        mutator.execute()
//      }
//
//      case None => futurePool {
//        // delete the entry
//        val mutator = HFactory.createMutator(keyspace, DynamicCompositeSerializer.get)
//        mutator.addDeletion(rowKey, columnFamilyName, colKey, DynamicCompositeSerializer.get)
//        mutator.execute()
//      }
//    }
//  }
//
//  def createDynamicKey[K: CassandraSerializable](orgKey: K): DynamicComposite = {
//    val result = new DynamicComposite
//    orgKey match {
//      case _: List[Any] => orgKey.asInstanceOf[List[Any]]
//        .foldLeft(0)((index, key) => { result.addComponent(key, keySerializers.lift(index).get); index + 1 })
//      case _ => result.addComponent(orgKey, keySerializers.lift(0).get)
//    }
//    result
//  }
//
//  override def get(key: (RK, CK)): Future[Option[V]] = {
//    val (rk, ck) = key
//    val rowKey = createDynamicKey(rk, keySerializer._1)
//    val colKey = createDynamicKey(ck, keySerializer._2)
//
//    futurePool {
//      val result = HFactory.createColumnQuery(keyspace, DynamicCompositeSerializer.get, DynamicCompositeSerializer.get, valueSerializer)
//        .setColumnFamily(columnFamilyName)
//        .setKey(rowKey)
//        .setName(colKey)
//        .execute()
//      if (result.get == null) None else Some(result.get.getValue)
//    }
//  }
//}
//
