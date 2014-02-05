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

/**
 *  This is a Storehaus-Cassandra wrapper for the well known Hector library often used in the JAVA-Cassandra world.
 *  Right now, this is a simple key value store. However, this is *not adequate* for a wide-column store,
 *  which is able to perform more complex operations like slice queries. See CassandraCompositeKeyStore for a different
 *  variant, which is more suitable for this type of operation.
 *  
 *  The apply method sets up the configuration:
 *  	* clusterName is the name of the cluster as in cassandra.yaml
 *      * keyspaceName is the name of the keyspace clients would like to use - here number of replicas should be already set up
 *      * columnFamilyName is the name of the column family to be used by this store
 *      * valueColumn is the name of the column to store the values in  
 * 
 *  At the moment - as long as i don't fully understand the consistency semantics of summingbird - we use consistency level Quorum 
 * 
 *  For this type of usage of Cassandra it doesn't make sense to provide multi-get and multi-put.
 *  
 *  WARNING: use at your own risk!
 * 
 *  @author Andreas Petter
 */

object CassandraKeyValueStore {

  def apply[K, V](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V]): CassandraKeyValueStore[K, V] = {
    apply(hostNames, clusterName, keyspaceName, columnFamilyName, keySerializer, valueSerializer, "value")
  }

  def apply[K, V](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Serializer[K], valueSerializer: Serializer[V], valueColumnName: String): CassandraKeyValueStore[K, V] = {
	val cassHostConfigurator = new CassandraHostConfigurator(hostNames)
	cassHostConfigurator.setRunAutoDiscoveryAtStartup(true)
	cassHostConfigurator.setAutoDiscoverHosts(true)
	cassHostConfigurator.setRetryDownedHosts(true)
    val cluster = HFactory.getOrCreateCluster(clusterName, cassHostConfigurator)
    val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
    new CassandraKeyValueStore[K, V](keyspace, columnFamilyName, keySerializer, valueSerializer, valueColumnName)
  }

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster
   * (throws an Exception if the keyspace is already existing)
   */
  def setupStore[K, V](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Serializer[K], replicationFactor: Int) = {
    val cluster : Cluster = HFactory.getOrCreateCluster(clusterName,hostNames)
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName, keySerializer.getComparatorType());
    val keyspace : KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
                                                                   Array(cfDef).toList)
    cluster.addKeyspace(keyspace, true)
  }
}

/**
 * trait marking Cassandra stores which allow to set consistency levels.
 * However, one should be aware of eventual consistency before lowering
 * consistency levels.
 */
trait CassandraConsistencyLevelableStore {
  def setConsistencyLevelPolicy(policy: ConsistencyLevelPolicy)
}

class CassandraKeyValueStore[K, V] (
		val keyspace : Keyspace, 
		val columnFamilyName: String,
		val keySerializer: Serializer[K],
		val valueSerializer: Serializer[V],
		val valueColumnName: String)
  extends Store[K, V] with CassandraConsistencyLevelableStore
{

  val futurePool = FuturePool(Executors.newFixedThreadPool(10))

  override def put(kv: (K, Option[V])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => futurePool {
        // write the new entry to Cassandra
        val mutator = HFactory.createMutator(keyspace, keySerializer)
        mutator.addInsertion(key, columnFamilyName, HFactory.createColumn(valueColumnName, value))
        mutator.execute()
      }

      case (key, None) => futurePool {
        // delete the entry
        val mutator = HFactory.createMutator(keyspace, keySerializer)
        mutator.addDeletion(key, columnFamilyName, valueColumnName, StringSerializer.get)
        mutator.execute()
      }
    }
  }

  override def get(key: K): Future[Option[V]] = {
    futurePool {
      val result = HFactory.createColumnQuery(keyspace, keySerializer, StringSerializer.get, valueSerializer)
          .setColumnFamily(columnFamilyName)
          .setKey(key)
          .setName(valueColumnName)
          .execute()
        if (result.get == null) None else Some(result.get.getValue)
    }
  }

  def setConsistencyLevelPolicy(policy: ConsistencyLevelPolicy) {
    keyspace.setConsistencyLevelPolicy(policy)
  }
}

