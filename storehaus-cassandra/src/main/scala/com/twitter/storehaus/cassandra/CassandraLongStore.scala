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
import com.twitter.util.{ Duration, Future }
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import me.prettyprint.hector.api.{ HConsistencyLevel, ConsistencyLevelPolicy, Cluster, Serializer }
import me.prettyprint.hector.api.beans.HCounterColumn
import me.prettyprint.hector.api.ddl.{ KeyspaceDefinition, ComparatorType }
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel
import me.prettyprint.cassandra.serializers.{ LongSerializer, StringSerializer }
import me.prettyprint.cassandra.service.ThriftKsDef
import scala.collection.JavaConversions._
import scala.util.Try
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy

/**
 * CassandraLongStore is *planned* as a *MergeableStore* for Longs
 * and therefore makes use of Cassandra counters, which allows merges
 * to be fast and eventually consistent.
 * It is advisable to use ConsistencyLevel.ONE for writes as documented
 * in http://www.datastax.com/docs/1.0/ddl/column_family#about-counter-columns
 * Be aware that in this case (Cassandra counters) put is an operation
 * that first reads a counters and then adds the inverse of the original
 * value + the new value, please read http://wiki.apache.org/cassandra/Counters.
 * This in turn introduces inconsistencies.
 * Set retry downed hosts to false to prevent over-counting through retries.
 *
 * @author Andreas Petter
 */
object CassandraLongStore {

  import ScalaSerializables._

  def apply[K: CassandraSerializable](
      keyspace: CassandraConfiguration.StoreKeyspace,
      columnFamily: CassandraConfiguration.StoreColumnFamily,
      valueColumnName: String = CassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
      policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_COUNTER_COLUMN_CONSISTENCY_LEVEL,
      sync: ExternalSynchronization = CassandraConfiguration.DEFAULT_SYNC,
      poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE): CassandraLongStore[K] = {
    val cassStore = CassandraStore[K, Long](keyspace, columnFamily, valueColumnName, policy, poolSize, None)
    new CassandraLongStore(cassStore, sync)
  }

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   *
   */
  def setupStore[K](
      cluster: CassandraConfiguration.StoreCluster,
      keyspaceName: String,
      columnFamily: CassandraConfiguration.StoreColumnFamily,
      keySerializer: Serializer[K],
      replicationFactor: Int) = {
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, keySerializer.getComparatorType());
    val keyspace: KeyspaceDefinition = HFactory.createKeyspaceDefinition(keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
      Array(cfDef).toList)
    cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName())
    cluster.getCluster.addKeyspace(keyspace, true)
  }

}

class CassandraLongStore[K](val underlying: CassandraStore[K, Long], val sync: ExternalSynchronization)
  /* extends ConvertedStore[K, K, Long, Long](underlying)(identity) */
  extends MergeableStore[K, Long] {

  def semigroup = implicitly[Semigroup[Long]]

  /**
   * For this to work the columnFamily needs to include counter columnns.
   * However the merge is kind of simple, as Cassandra supports counter
   * columns which means that we just place an insertion and we are done
   * even with ConsistencyLevel.ONE
   */
  override def merge(kv: (K, Long)) = {
    val (key, value) = kv
    val lockId = "lock/" +
      underlying.keyspace.getKeyspace.getKeyspaceName + "/" +
      underlying.columnFamily.name + "/" +
      key.toString
    val counterColumn: HCounterColumn[String] = HFactory.createCounterColumn(underlying.valueColumnName, value, StringSerializer.get);
    underlying.futurePool {
      sync.lock(lockId, Future {
        val mutator = HFactory.createMutator(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer)
        mutator.insertCounter(key, underlying.columnFamily.name, counterColumn)
        mutator.execute()
        val counters = HFactory.createCounterColumnQuery(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer, StringSerializer.get)
        counters.setKey(key)
        counters.setColumnFamily(underlying.columnFamily.name)
        counters.setName(underlying.valueColumnName)
        val result = counters.execute()
        Option(result.get).map(_.getValue - value)
      }).get
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
    val (key, optvalue) = kv
    val lockId = "lock/" +
      underlying.keyspace.getKeyspace.getKeyspaceName + "/" +
      underlying.columnFamily.name + "/" +
      key.toString
    optvalue match {
      case Some(value) => underlying.futurePool {
        sync.lock(lockId, Future {
          // read value, if existing
          val counters = HFactory.createCounterColumnQuery(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer, StringSerializer.get);
          counters.setKey(key);
          counters.setColumnFamily(underlying.columnFamily.name);
          counters.setName(underlying.valueColumnName);
          val result = counters.execute();

          // write the inverse or new entry to Cassandra
          val resettedvalue = if (result.get == null) value else value - result.get().getValue()
          val counterColumn1: HCounterColumn[String] = HFactory.createCounterColumn(underlying.valueColumnName, resettedvalue, StringSerializer.get)
          val mutator = HFactory.createMutator(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer)
          mutator.addCounter(key, underlying.columnFamily.name, counterColumn1)
          mutator.execute()
        })
      }

      case None => underlying.futurePool {
        sync.lock(lockId, Future {
          // delete the entry
          val mutator = HFactory.createMutator(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer)
          mutator.deleteCounter(key, underlying.columnFamily.name, underlying.valueColumnName, StringSerializer.get)
          mutator.execute()
        })
      }
    }
  }

  override def get(key: K): Future[Option[Long]] = {
    underlying.futurePool {
      val result = HFactory.createCounterColumnQuery(underlying.keyspace.getKeyspace, underlying.keySerializer.getSerializer, StringSerializer.get)
        .setColumnFamily(underlying.columnFamily.name)
        .setKey(key)
        .setName(underlying.valueColumnName)
        .execute()
      Option(result.get).map(_.getValue)
    }
  }

}


