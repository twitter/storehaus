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
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayOps
import com.twitter.storehaus.{ Store, WithPutTtl }
import com.twitter.util.{ Duration, Future, FuturePool }
import ScalaSerializables._
import me.prettyprint.cassandra.model.HColumnImpl
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer
import me.prettyprint.cassandra.service.ThriftKsDef
import me.prettyprint.hector.api.{ Cluster, Serializer, Keyspace, ConsistencyLevelPolicy }
import me.prettyprint.hector.api.beans.{ DynamicComposite, HColumn, AbstractComposite }
import me.prettyprint.hector.api.ddl.{ ComparatorType, KeyspaceDefinition }
import me.prettyprint.hector.api.factory.HFactory
import javax.naming.OperationNotSupportedException
import com.twitter.util.ExecutorServiceFuturePool

/**
 *  Hector-Cassandra wrapper using dynamic composite keys.
 *  For convenience type parameters are not provided, except for the type of the value.
 *  The serializers needed for the composite key are provided using a Seq of functions (serializerList).
 *  Serializers for the most common types are already included by default. 
 *  @author Andreas Petter
 */

object CassandraDynamicCompositeStore {

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   * (shouldn't work on on existing keyspaces and column-families)
   */
  def setupStore[V: CassandraSerializable](
      cluster: CassandraConfiguration.StoreCluster, 
      keyspaceName: String, 
      columnFamily: CassandraConfiguration.StoreColumnFamily) = {
    val valueSerializer = implicitly[CassandraSerializable[V]]
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, ComparatorType.DYNAMICCOMPOSITETYPE);
    cfDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setKeyValidationClass(ComparatorType.DYNAMICCOMPOSITETYPE.getClassName())
    cfDef.setKeyValidationAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES)
    cfDef.setDefaultValidationClass(valueSerializer.getSerializer.getComparatorType().getClassName())
    cluster.getCluster.addColumnFamily(cfDef, true)
  }
}

class CassandraDynamicCompositeStore[V: CassandraSerializable] (
  val keyspace: CassandraConfiguration.StoreKeyspace,
  val columnFamily: CassandraConfiguration.StoreColumnFamily,
  val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION,
  val serializerList: Seq[Any => Option[CassandraSerializable[_]]] = CassandraConfiguration.DEFAULT_SERIALIZER_LIST)
  	extends Store[(Any, Any), V] 
    with WithPutTtl[(Any, Any), V, CassandraDynamicCompositeStore[V]] 
    with CassandraPartiallyIterableStore[Any, Any, Unit, Unit, V, DynamicComposite] {
  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)
  
  val valueSerializer = implicitly[CassandraSerializable[V]]
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  override def withPutTtl(ttl: Duration): CassandraDynamicCompositeStore[V] = new CassandraDynamicCompositeStore[V](keyspace,
    columnFamily, policy, poolSize, Option(ttl), serializerList)

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
    	      val column = new HColumnImpl(colKey, value, HFactory.createClock, DynamicCompositeSerializer.get, valueSerializer.getSerializer)
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
        .foreach(key => { 
          val dserializer = ScalaSerializables.getSerializerForEntity(key, serializerList).get
          result.addComponent(key, dserializer.getSerializer.asInstanceOf[Serializer[Any]], dserializer.getSerializer.getComparatorType.getTypeName, AbstractComposite.ComponentEquality.fromByte(0))
          })
      case _ => {
        val dserializer = ScalaSerializables.getSerializerForEntity(orgKey, serializerList).get
        result.addComponent(orgKey, dserializer.getSerializer.asInstanceOf[Serializer[Any]], dserializer.getSerializer.getComparatorType.getTypeName, AbstractComposite.ComponentEquality.fromByte(0))
      }
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
  
  // implementation of methods used by CassandraPartiallyIterableStore 
  override def createColKey(key: Any): DynamicComposite = createDynamicKey(key)
  override def createRowKey(key: Any): DynamicComposite = createDynamicKey(key)
  override def getCompositeSerializer: Serializer[DynamicComposite] = DynamicCompositeSerializer.get
  override def getValueSerializer: Serializer[V] = valueSerializer.getSerializer
  override def getRowResult(list: List[AbstractComposite#Component[_]]): Any = decideOutput(list) 
  override def getColResult(list: List[AbstractComposite#Component[_]]): Any = decideOutput(list)
  override def getFuturePool: ExecutorServiceFuturePool = futurePool
  override def getKeyspace: Keyspace = keyspace.getKeyspace
  override def getColumnFamilyName: String = columnFamily.name
 
  private def decideOutput(input: List[AbstractComposite#Component[_]]): Any = {
    def walkList(list: List[AbstractComposite#Component[_]]): List[Any] = {
      if(list.nonEmpty)
    	list.head.getValue :: walkList(list.tail) 
      else
        Nil
    }
    if (input.size == 1) input.last else input 
  }
}

