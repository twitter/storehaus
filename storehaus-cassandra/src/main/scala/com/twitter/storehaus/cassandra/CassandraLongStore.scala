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

import java.util.{ Map => JMap }
import com.twitter.algebird.Semigroup
import com.twitter.bijection.Conversion.asMethod
import com.twitter.util.Future
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Serializer
import scala.util.Try
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.cassandra.service.ThriftKsDef
import scala.collection.JavaConversions._
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel
import me.prettyprint.hector.api.HConsistencyLevel
import me.prettyprint.hector.api.ConsistencyLevelPolicy
import me.prettyprint.hector.api.beans.HCounterColumn

/**
 * An implementation of MergeableStore for type Long of CassandraStore
 * 
 * CassandraLongStore is *planned* as a *MergeableStore* and therefore  
 * makes use of Cassandra counters, which allows merges to be fast and
 * eventually consistent while retaining ConsistencyLevel.ONE.
 * Be aware that in this case (Cassandra counters) put is an operation 
 * that first reads a counters and then adds the inverse of the original
 * value + the new value.
 * This in turn introduces consistency lacks.
 * 
 * 
 * WARNING: use at your own risk!
 * 
 * @author Andreas Petter
 */
object CassandraLongStore {
  def apply[K](hostNames: String, clusterName: String, keyspaceName : String, keySerializer: Serializer[K], columnFamilyName: String) : CassandraLongStore[K] =
    apply(hostNames, clusterName, keyspaceName, columnFamilyName, keySerializer, "value")
    
  def apply[K](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Serializer[K], valueColumnName: String) : CassandraLongStore[K] = {
    val cassStore = CassandraKeyValueStore(hostNames, clusterName, keyspaceName, columnFamilyName, keySerializer, ScalaLongSerializer(), valueColumnName)
    val policy = new ConfigurableConsistencyLevel();
	policy.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
	policy.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
	cassStore.setConsistencyLevelPolicy(policy);
	new CassandraLongStore(cassStore)
  }
  
  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   * 
   */
  def setupStore[K](hostNames: String, clusterName: String, keyspaceName : String, columnFamilyName: String, keySerializer: Serializer[K], replicationFactor: Int) = {
    val cluster : Cluster = HFactory.getOrCreateCluster(clusterName,hostNames)
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamilyName, keySerializer.getComparatorType());
    cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName())
    val keyspace : KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
                                                                   Array(cfDef).toList)
    cluster.addKeyspace(keyspace, true)
  }

  /**
   *
   * 
   */
}

class CassandraLongStore[K](underlying: CassandraKeyValueStore[K, Long])
  extends ConvertedStore[K, K, Long, Long](underlying)(identity)
  with MergeableStore[K, Long] {

  def semigroup = implicitly[Semigroup[Long]]
  
  /**
   * For this to work the columnFamily needs to include counter columnns.
   * However the merge is kind of simple, as Cassandra supports counter
   * columns which means that we just place an insertion and we are done
   * even with ConsistencyLevel.ONE
   */
  override def merge(kv: (K, Long)) = {
    val counterColumn: HCounterColumn[String] = HFactory.createCounterColumn(underlying.valueColumnName, kv._2, StringSerializer.get);
    underlying.futurePool {
      val mutator = HFactory.createMutator(underlying.keyspace, underlying.keySerializer)
      mutator.insertCounter(kv._1, underlying.columnFamilyName, counterColumn)
      mutator.execute()     
      val counters = HFactory.createCounterColumnQuery(underlying.keyspace, underlying.keySerializer, StringSerializer.get)
      counters.setKey(kv._1);
      counters.setColumnFamily(underlying.columnFamilyName);
      counters.setName(underlying.valueColumnName);
      val result = counters.execute();
      if (result.get == null) None else Some(result.get.getValue - kv._2)
    }
  }
  
  /**
   * Do the store operation by reading the value and writing the inverse value.
   * This is needed because Cassandra does not support put in case of counter
   * columns. Be aware that using counter columns usually involves consistencyLevel
   * ONE, i.e. the values might be outdated and a put actually may put a wrong
   * value.
   */ 
  override def put(kv: (K, Option[Long])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => underlying.futurePool {
        // read value, if existing
        val counters = HFactory.createCounterColumnQuery(underlying.keyspace, underlying.keySerializer, StringSerializer.get);
        counters.setKey(kv._1);
        counters.setColumnFamily(underlying.columnFamilyName);
        counters.setName(underlying.valueColumnName);
        val result = counters.execute();

        // write the inverse or new entry to Cassandra
        val resettedvalue = if(result.get == null) value else value - result.get().getValue()
       	val counterColumn1: HCounterColumn[String] = HFactory.createCounterColumn(underlying.valueColumnName, resettedvalue, StringSerializer.get)
       	val mutator = HFactory.createMutator(underlying.keyspace, underlying.keySerializer)
       	mutator.addCounter(key, underlying.columnFamilyName, counterColumn1)
       	mutator.execute()
      }

      case (key, None) => underlying.futurePool {
        // delete the entry
        val mutator = HFactory.createMutator(underlying.keyspace, underlying.keySerializer)
        mutator.deleteCounter(key, underlying.columnFamilyName, underlying.valueColumnName, StringSerializer.get)
        mutator.execute()
      }
    }
  }

  override def get(key: K): Future[Option[Long]] = {
    underlying.futurePool {
      val result = HFactory.createCounterColumnQuery(underlying.keyspace, underlying.keySerializer, StringSerializer.get)
          .setColumnFamily(underlying.columnFamilyName)
          .setKey(key)
          .setName(underlying.valueColumnName)
          .execute()
       if (result.get == null) None else Some(result.get.getValue)
    }
  }

  
}


