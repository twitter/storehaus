/*
 * Copyright 2014 SEEBURGER AG
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
package com.twitter.storehaus.cassandra

import java.util.concurrent.Executors
import com.twitter.util.{ Future, FuturePool }
import com.twitter.storehaus.Store
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.{ Cluster, Keyspace, HConsistencyLevel, ConsistencyLevelPolicy, Serializer }
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.ddl.{ ComparatorType, KeyspaceDefinition }
import me.prettyprint.hector.api.exceptions.HNotFoundException
import me.prettyprint.hector.api.beans.DynamicComposite
import me.prettyprint.cassandra.model.{ QuorumAllConsistencyLevelPolicy, ConfigurableConsistencyLevel }
import me.prettyprint.cassandra.serializers.{ DynamicCompositeSerializer, StringSerializer }
import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, ThriftKsDef }
import scala.collection.JavaConversions._
import ScalaSerializables._
import com.twitter.util.Duration
import com.twitter.storehaus.FutureOps
import scala.collection.mutable.ArrayOps
import com.twitter.storehaus.WithPutTtl

/**
 *  A Cassandra wrapper using dynamic composite keys
 *
 *  The Store variant provided here has the ability to do slice queries over column slices, which allows
 *  for better performance and unknown column names (i.e. to store values in column names).
 *
 *  For convenience type parameters are more flexible than needed. One can pass in a bunch of
 *  Cassandra/Hector-serializbale objects and the corresponding serializers:
 *  	RK: represents the type(s) of the Cassandra-composite-row-key.
 *   		One can either pass in a single type or a List of types.
 *     		Remember it might not possible to peform range scans with random-partitioners like Murmur3.
 *   	CK: represents the type(s) of the Cassandra-composite-column-key.
 *    		See RK for details.
 *      	It is possible to perform queries on column slices.
 *       V: The type of the value.
 *
 *  @author Andreas Petter
 */

object CassandraDynamicCompositeStore {

//  def apply[RK, CK, V: CassandraSerializable](
//    keyspace: CassandraConfiguration.StoreKeyspace,
//    columnFamily: CassandraConfiguration.StoreColumnFamily,
//    rowKeySerializers: List[CassandraSerializable],
//    columnKeySerializers: List[CassandraSerializable],
//    policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
//    poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
//    ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION): CassandraDynamicCompositeStore[RK, CK, V] =
//    new CassandraDynamicCompositeStore[RK, CK, V](keyspace, columnFamily, valueSerializer, policy, poolSize, ttl)

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   * (shouldn't work on on existing keyspaces and column-families)
   */
  def setupStore[V: CassandraSerializable](
      cluster: CassandraConfiguration.StoreCluster, 
      keyspaceName: String, 
      columnFamily: CassandraConfiguration.StoreColumnFamily,  
      replicationFactor: Int) = {
    val valueSerializer = implicitly[CassandraSerializable[V]]
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, ComparatorType.DYNAMICCOMPOSITETYPE);
    cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setKeyValidationClass(ComparatorType.DYNAMICCOMPOSITETYPE.getClassName())
    cfDef.setKeyValidationAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setDefaultValidationClass(valueSerializer.getSerializer.getComparatorType().getClassName())
    val keyspace: KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
      Array(cfDef).toList)
    cluster.getCluster.addKeyspace(keyspace, true)
  }
}

class CassandraDynamicCompositeStore[V: CassandraSerializable] (
  val keyspace: CassandraConfiguration.StoreKeyspace,
  val columnFamily: CassandraConfiguration.StoreColumnFamily,
  val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION,
  val serializerList: Seq[Any => Option[CassandraSerializable[_]]] = CassandraConfiguration.DEFAULT_SERIALIZER_LIST)
  	extends Store[(Any, Any), V] with WithPutTtl[(Any, Any), V, CassandraDynamicCompositeStore[V]] {
  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)
  
  val valueSerializer = implicitly[CassandraSerializable[V]]
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  override def withPutTtl(ttl: Duration): CassandraDynamicCompositeStore[V] = new CassandraDynamicCompositeStore[V](keyspace,
    columnFamily, policy, poolSize, Option(ttl), serializerList)

  // override def put(kv: ((Any, Any), Option[V])): Future[Unit] = {
  override def multiPut[K1 <: Any](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool {
    	val mutator = HFactory.createMutator(keyspace.getKeyspace, DynamicCompositeSerializer.get)
    	kvs.foreach{ kv =>
          // create the keys for rows and columns
    	  val ((rk, ck), valueOpt) = kv
    	  val rowKey = createDynamicKey(rk)
    	  val colKey = createDynamicKey(ck)
    	  valueOpt match {
    	    case Some(value) => {
    	      val column = HFactory.createColumn(colKey, value)
    	      ttl match { 
    	        case Some(duration) => column.setTtl(duration.inSeconds)
    	        case _ =>
    	      }
      	      mutator.addInsertion(rowKey, columnFamily.name, column)
    	    }
    	    case None => mutator.addDeletion(rowKey, columnFamily.name, colKey, DynamicCompositeSerializer.get)
    	  }
    	}
    	val res = mutator.execute()
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  private def createDynamicKey(orgKey: Any): DynamicComposite = {
    val result = new DynamicComposite
    orgKey match {
      // most collections fit into the first case
      case _: Traversable[_] => orgKey.asInstanceOf[Traversable[Any]]
        .foreach(key => { result.addComponent(key, ScalaSerializables.getSerializerForEntity(key, serializerList).get.getSerializer.asInstanceOf[Serializer[Any]])})
      case _ => result.addComponent(orgKey, ScalaSerializables.getSerializerForEntity(orgKey, serializerList).get.getSerializer.asInstanceOf[Serializer[Any]])
    }
    result
  }

  override def get(key: (Any, Any)): Future[Option[V]] = {
    val (rk, ck) = key
    val rowKey = createDynamicKey(rk)
    val colKey = createDynamicKey(ck)

    futurePool {
      val result = HFactory.createColumnQuery(keyspace.getKeyspace, DynamicCompositeSerializer.get, DynamicCompositeSerializer.get, valueSerializer.getSerializer)
        .setColumnFamily(columnFamily.name)
        .setKey(rowKey)
        .setName(colKey)
        .execute()
      if (result.get == null) None else Some(result.get.getValue)
    }
  }
  
//  override def multiGet[K1 <: (Any, Any)](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
//    import ArrayOps._
//    val future = futurePool {
//      ArrayOps.toArray(ks). .toBuffer[(Any,Any)].a.sortBy(_._1)
//    }
//    
//    FutureOps.liftValues(ks,
//      future
//      ,
//      { (k: K1) => Future.None })
//  }
}

