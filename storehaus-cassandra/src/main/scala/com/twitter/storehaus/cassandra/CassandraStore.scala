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

import com.twitter.util.{Duration, Future, FuturePool}
import com.twitter.storehaus.{Store, WithPutTtl}
import java.util.concurrent.Executors
import me.prettyprint.cassandra.service.ThriftKsDef
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.{Serializer, ConsistencyLevelPolicy, Cluster}
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.hector.api.factory.HFactory
import scala.collection.JavaConversions._
import scala.compat.Platform

/**
 *  This is a Storehaus-Cassandra wrapper for the well known Hector library often used in the JAVA-Cassandra world.
 *  Right now, this is a simple key value store. However, this is *not adequate* for a wide-column store,
 *  which is able to perform more complex operations like slice queries. See CassandraCompositeKeyStore for a different
 *  variant, which is more suitable for this type of operation.
 *  
 *  @author Andreas Petter
 */

object CassandraStore {

  /**
   * Setup configuration
   *    * keyspace contains the name of the keyspace clients would like to use 
   *    	- here cluster and number of replicas should be already set up
   *    * columnFamily contains the name of the column family to be used by this store
   *    * key- and valueSerializers need to be specified 
   * Optional parameters
   *    * valueColumn is the name of the column to store the values in ("value")
   *    * policy can be used to set a ConsistencyLevel other than Quorum
   *    * poolSize can be used to define the size of the future thread pool (10)
   *    * ttl be set can for automatic column deletion, None = infinite lifetime :) (None)
   */
  def apply[K : CassandraSerializable, V : CassandraSerializable](
      keyspace: CassandraConfiguration.StoreKeyspace, 
      columnFamily: CassandraConfiguration.StoreColumnFamily, 
      valueColumnName: String = CassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME, 
      policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL, 
      poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
      ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION): CassandraStore[K, V] = 
    new CassandraStore[K, V](keyspace, columnFamily, valueColumnName, policy, poolSize, ttl)
  

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster
   * (throws an Exception if the keyspace is already existing)
   */
  def setupStore[K : CassandraSerializable](
      cluster: CassandraConfiguration.StoreCluster, 
      keyspaceName : String, 
      columnFamily: CassandraConfiguration.StoreColumnFamily) = {
    val keySerializer = implicitly[CassandraSerializable[K]]
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, keySerializer.getSerializer.getComparatorType());
    cluster.getCluster.addColumnFamily(cfDef, true)
  }

}

class CassandraStore[K : CassandraSerializable, V : CassandraSerializable] (
		val keyspace : CassandraConfiguration.StoreKeyspace, 
		val columnFamily: CassandraConfiguration.StoreColumnFamily,
		val valueColumnName: String = CassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
		val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION)
  extends Store[K, V] with WithPutTtl[K, V, CassandraStore[K, V]]
{
  val keySerializer = implicitly[CassandraSerializable[K]]
  val valueSerializer = implicitly[CassandraSerializable[V]]
  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  override def withPutTtl(ttl: Duration): CassandraStore[K, V] = CassandraStore(keyspace, 
      columnFamily, valueColumnName, policy, poolSize, Option(ttl))
  
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool {
    	val mutator = HFactory.createMutator(keyspace.getKeyspace, keySerializer.getSerializer)
    	kvs.foreach{
    	  case (key, Some(value)) => {
    	    val column = HFactory.createColumn(valueColumnName, value)
    	    ttl match { 
    	      case Some(duration) => column.setTtl(duration.inSeconds)
    	      case _ =>
    	    }
    	    mutator.addInsertion(key, columnFamily.name, column)
    	  }
    	  case (key, None) => mutator.addDeletion(key, columnFamily.name, valueColumnName, StringSerializer.get)
    	}
    	val res = mutator.execute()
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  override def get(key: K): Future[Option[V]] = {
    futurePool {
      val result = HFactory.createColumnQuery(
    		  keyspace.getKeyspace, 
    		  keySerializer.getSerializer, 
    		  StringSerializer.get, 
    		  valueSerializer.getSerializer)
          .setColumnFamily(columnFamily.name)
          .setKey(key)
          .setName(valueColumnName)
          .execute()
        Option(result.get).map(_.getValue)
    }
  }
}
