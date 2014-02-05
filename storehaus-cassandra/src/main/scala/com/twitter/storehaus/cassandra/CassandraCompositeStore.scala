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
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.hector.api.{Cluster, Keyspace}
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.cassandra.service.ThriftKsDef
import scala.collection.JavaConversions._
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel
import me.prettyprint.hector.api.HConsistencyLevel
import me.prettyprint.hector.api.ConsistencyLevelPolicy
import me.prettyprint.hector.api.exceptions.HNotFoundException
import me.prettyprint.hector.api.beans.DynamicComposite
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer

/**
 *  This is a Storehaus-Cassandra wrapper for dynamic composite keys provided by the well known Hector library 
 *  often used in the JAVA-Cassandra world.
 *  
 *  The Store variant provided here has the ability to do slice queries over column slices, which allows
 *  for better performance and unknown column names (i.e. to store values in column names).
 *  
 *  For an explanation of input params please consult CassandraKeyValueStore
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
 *  In any case (even if one only uses a single row or a single column key) it is imperative to pass
 *  in the correct Cassandra/Hector serializers as a list. The list should be ordered exactly according to
 *  the keys (i.e. RK and CK). 
 *  
 * 	WARNING: use at your own risk!
 * 
 *  @author Andreas Petter
 */

object CassandraCompositeWideColumnStore {

  def apply[RK, CK, V](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Tuple2[List[Serializer[Any]], List[Serializer[Any]]], valueSerializer: Serializer[V]): CassandraCompositeWideColumnStore[RK, CK, V] = {
	val cassHostConfigurator = new CassandraHostConfigurator(hostNames)
	cassHostConfigurator.setRunAutoDiscoveryAtStartup(true)
	cassHostConfigurator.setAutoDiscoverHosts(true)
	cassHostConfigurator.setRetryDownedHosts(true)
    val cluster = HFactory.getOrCreateCluster(clusterName, cassHostConfigurator)
    val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
    new CassandraCompositeWideColumnStore[RK, CK, V](keyspace, columnFamilyName, keySerializer, valueSerializer)
  }

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   * (shouldn't work on on existing keyspaces and column-families)
   */
  def setupStore[RK, CK, V](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Tuple2[List[Serializer[Any]], List[Serializer[Any]]], valueSerializer: Serializer[V], replicationFactor: Int) = {
    val cluster : Cluster = HFactory.getOrCreateCluster(clusterName,hostNames)
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName, ComparatorType.DYNAMICCOMPOSITETYPE)
    cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setKeyValidationClass(ComparatorType.DYNAMICCOMPOSITETYPE.getClassName())
    cfDef.setKeyValidationAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getClassName())
    val keyspace : KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
                                                                   Array(cfDef).toList)
    cluster.addKeyspace(keyspace, true)
  }
}

class CassandraCompositeWideColumnStore[RK, CK, V] (
		val keyspace : Keyspace, 
		val columnFamilyName: String,
		val keySerializer: Tuple2[List[Serializer[Any]], List[Serializer[Any]]],
		val valueSerializer: Serializer[V])
  extends Store[(RK, CK), V] with CassandraConsistencyLevelableStore
{

  val futurePool = FuturePool(Executors.newFixedThreadPool(10))

  
  override def put(kv: ((RK, CK), Option[V])): Future[Unit] = {
    // create the keys for rows and columns
    val ((rk, ck),valueOpt) = kv
    val rowKey = createDynamicKey(rk, keySerializer._1)
    val colKey = createDynamicKey(ck, keySerializer._2)

    valueOpt match {
      case Some(value) 	=> futurePool {
        // write the new entry to Cassandra
        val mutator = HFactory.createMutator(keyspace, DynamicCompositeSerializer.get)
        mutator.addInsertion(rowKey, columnFamilyName, HFactory.createColumn(colKey, value))
        mutator.execute()
      }

      case None 		=> futurePool {
        // delete the entry
        val mutator = HFactory.createMutator(keyspace, DynamicCompositeSerializer.get)
        mutator.addDeletion(rowKey, columnFamilyName, colKey, DynamicCompositeSerializer.get)
        mutator.execute()
      }
    }
  } 

  def createDynamicKey(orgKey: Any, keySerializers: List[Serializer[Any]]):DynamicComposite = {
    val result = new DynamicComposite
    orgKey match {
      case _:List[Any] 	=> orgKey.asInstanceOf[List[Any]]
    		  					.foldLeft(0)((index,key) => { result.addComponent(key, keySerializers.lift(index).get); index + 1 } )
      case _ 			=> result.addComponent(orgKey, keySerializers.lift(0).get)
    }
    result
  }
  
  override def get(key: (RK, CK)): Future[Option[V]] = {
    val (rk, ck) = key
    val rowKey = createDynamicKey(rk, keySerializer._1)
    val colKey = createDynamicKey(ck, keySerializer._2)

    futurePool {
      val result = HFactory.createColumnQuery(keyspace, DynamicCompositeSerializer.get, DynamicCompositeSerializer.get, valueSerializer)
          .setColumnFamily(columnFamilyName)
          .setKey(rowKey)
          .setName(colKey)
          .execute()
        if (result.get == null) None else Some(result.get.getValue)
    }
  }

  def setConsistencyLevelPolicy(policy: ConsistencyLevelPolicy) {
    keyspace.setConsistencyLevelPolicy(policy)
  }
}

