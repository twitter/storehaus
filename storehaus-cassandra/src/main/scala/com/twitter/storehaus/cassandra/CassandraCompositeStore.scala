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
//import com.twitter.util.{ Duration, Future, FuturePool }
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
//import shapeless._
//import Nat._
//import UnaryTCConstraint._
//
///**
// *  Storehaus-Cassandra wrapper for dynamic composite keys provided by the well known Hector library
// *  often used in the JAVA-Cassandra world.
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
// *  In any case (even if one only uses a single row or a single column key) it is imperative to pass
// *  in the correct Cassandra/Hector serializers as a list. The list should be ordered exactly according to
// *  the keys (i.e. RK and CK).
// *
// *  @author Andreas Petter
// */
//
//object CassandraCompositeStore {
//
////  def apply[RK <: HList, CK <: HList, V : CassandraSerializable, RS <: HList, CS <: HList](
////      keyspace: CassandraConfiguration.StoreKeyspace,
////      columnFamily: CassandraConfiguration.StoreColumnFamily,
////      keySerializers: RS,
////      colSerializers: CS,
////      policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
////      poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
////      ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION): CassandraCompositeStore[RK, CK, V, RS, CS] = {
////    new CassandraCompositeStore[RK, CK, V, RS, CS](
////        keyspace, 
////        columnFamily, 
////        keySerializers, 
////        colSerializers, 
////        policy, 
////        poolSize, 
////        ttl)
////  }
//
//  
//  /**
//   * Optionally this method can be used to setup storage on the Cassandra cluster.
//   * (shouldn't work on on existing keyspaces and column-families)
//   */
////  def setupStore[RK <: HList, CK <: HList, V](
////      hostNames: String, 
////      clusterName: String, 
////      keyspaceName: String, 
////      columnFamilyName: String, 
////      keySerializers: RK,
////      colSerializers: CK,
////      valueSerializer: Serializer[V], 
////      replicationFactor: Int) = {
////    val cluster: Cluster = HFactory.getOrCreateCluster(clusterName, hostNames)
////    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName, ComparatorType.COMPOSITETYPE)
////    object compositeStringMapping extends (CassandraSerializable ~>> String) {
////	  override def apply[T](c: CassandraSerializable[T]) = c.getSerializer.getComparatorType().getTypeName()
////  	}
////    cfDef.setComparatorTypeAlias("(" + keySerializers.map(compositeStringMapping).toList[String].mkString + ")")
////    cfDef.setKeyValidationClass(ComparatorType.COMPOSITETYPE.getTypeName())
////    cfDef.setKeyValidationAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
////    cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getClassName())
////    val keyspace: KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
////      Array(cfDef).toList)
////    cluster.addKeyspace(keyspace, true)
////  }
//}
//
//class CassandraCompositeStore[
//                              RK <: HList, 
//                              CK <: HList, 
//                              V : CassandraSerializable, 
//                              RS <: HList : *->*[CassandraSerializable]#λ, 
//                              CS <: HList : *->*[CassandraSerializable]#λ](
//    val keyspace : CassandraConfiguration.StoreKeyspace, 
//    val columnFamily: CassandraConfiguration.StoreColumnFamily,
//    val keySerializer: RS,
//    val colSerializer: CS,
//    val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
//    val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
//    val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION)
//    (implicit evrow: MappedAux[RK, CassandraSerializable, RS], evcol: MappedAux[CK, CassandraSerializable, CS])
//  extends Store[(RK, CK), V] {
//  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)
//  val valueSerializer = implicitly[CassandraSerializable[V]]
//  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))
//
//  override def put(kv: ((RK, CK), Option[V])): Future[Unit] = {
//    // create the keys for rows and columns
//    val ((rk, ck), valueOpt) = kv
//    val rowKey = createDynamicKey(rk, keySerializer)
//    val colKey = createDynamicKey(ck, colSerializer)
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
//  object hlistMapperRK extends (~) {
//    
//  }
//  
//  def createDynamicRowKey(orgKey: RK, keySerializers: RS): DynamicComposite = {
//	object hlistMapperRK extends (~) {
//		def apply[T](s: List[T]) : String = s match {
//		case l : List[_] => l.toString
//		case _ => "else"
//	}
//    val result = new DynamicComposite
//    orgKey.map()
//    
//    orgKey match {
//      case _: List[Any] => orgKey.asInstanceOf[List[Any]]
//        .foldLeft(0)((index, key) => { result.addComponent(key, keySerializers.a 
//            keySerializers.lift(index).get); index + 1 })
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
//
//}
//
