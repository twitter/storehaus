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

import com.twitter.storehaus.{ Store, WithPutTtl }
import com.twitter.util.{ Duration, Future, FuturePool, ExecutorServiceFuturePool }
import java.util.concurrent.Executors
import javax.naming.OperationNotSupportedException
import me.prettyprint.hector.api.{ Cluster, Keyspace, HConsistencyLevel, ConsistencyLevelPolicy, Serializer }
import me.prettyprint.hector.api.beans.{ HColumn, Composite, AbstractComposite }
import me.prettyprint.hector.api.ddl.{ ComparatorType, KeyspaceDefinition }
import me.prettyprint.hector.api.exceptions.HNotFoundException
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.service.{ CassandraHostConfigurator, ThriftKsDef }
import scala.collection.JavaConversions._
import shapeless._
import HList._
import Traversables._
import Nat._
import UnaryTCConstraint._

/**
 *  Cassandra store for fixed composite keys, allows to do slice queries over column slices.
 *  Row- and Column-keys are provided as HList to achieve type safety.
 *  @author Andreas Petter
 */

object CassandraCompositeStore {
  
  /**
   * used for mapping the keys in the HLists to Strings
   */
  private object compositeStringMapping extends (CassandraSerializable ~>> String) {
    override def apply[T](c: CassandraSerializable[T]) = c.getSerializer.getComparatorType().getTypeName()
  }

  /**
   * Optionally this method can be used to setup storage on the Cassandra cluster.
   * (shouldn't work on on existing keyspaces and column-families)
   */
  def setupStore[RS <: HList, CS <: HList, V, MRKResult <: HList, MCKResult <: HList](
    cluster: CassandraConfiguration.StoreCluster,
    keyspaceName: String,
    columnFamily: CassandraConfiguration.StoreColumnFamily,
    keySerializers: RS,
    colSerializers: CS,
    valueSerializer: Serializer[V])
    (implicit mrk: MapperAux[compositeStringMapping.type, RS, MRKResult],
      mck: MapperAux[compositeStringMapping.type, CS, MCKResult],
      tork: ToList[MRKResult, String],
      tock: ToList[MCKResult, String],
      rsUTC: *->*[CassandraSerializable]#λ[RS],
      csUTC: *->*[CassandraSerializable]#λ[CS],
      cassSerValue: CassandraSerializable[V]) = {
    val cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, columnFamily.name, ComparatorType.COMPOSITETYPE)
    cfDef.setComparatorTypeAlias("(" + colSerializers.map(compositeStringMapping).toList.mkString(",") + ")")
    cfDef.setKeyValidationClass(ComparatorType.COMPOSITETYPE.getTypeName())
    cfDef.setKeyValidationAlias("(" + keySerializers.map(compositeStringMapping).toList.mkString(",") + ")")
    cfDef.setDefaultValidationClass(valueSerializer.getComparatorType().getClassName())
    cluster.getCluster.addColumnFamily(cfDef, true)
  }
  
  /**
   * used to map over an example key to implicit serializers
   */
  private object compositeSerializerCreation extends Poly1 {
    implicit def default[T : CassandraSerializable] = at[T](_ => implicitly[CassandraSerializable[T]])
  }
  
  /**
   * creates an HList of serializers given an example key HList and pulls serializers out of implicit scope
   */
  def getSerializerHListByExample[KL <: HList, SL <: HList](keys: KL)(implicit mapper: MapperAux[compositeSerializerCreation.type, KL, SL]) : SL = {
    keys.map(compositeSerializerCreation)
  } 
  
  
  /**
   * helper trait for declaring the HList recursive function 
   * to append keys on a Composite in a type safe way
   */
  trait Append2Composite[R <: Composite, K <: HList, S <: HList] {
    def apply(r: R, k: K, s: S): Unit
  }
  
  /**
   * helper implicits for the recursion itself
   */
  object Append2Composite {
    implicit def hnilAppend2Composite[R <: Composite] = new Append2Composite[R, HNil, HNil] {
      override def apply(r: R, k: HNil, s: HNil): Unit = {}
    }
    implicit def hlistAppend2Composite[R <: Composite, M, K <: HList, N, S <: HList](
      implicit a2c: Append2Composite[R, K, S]) = new Append2Composite[R, M :: K, N :: S] {
    	override def apply(r: R, k: M :: K, s: N :: S): Unit = {
    	  // the following cast is safe because we verified the HList-types using MappedAux
    	  val serializer = s.head.asInstanceOf[CassandraSerializable[M]].getSerializer.asInstanceOf[Serializer[M]]
    	  r.addComponent[M](k.head, serializer, serializer.getComparatorType.getTypeName, AbstractComposite.ComponentEquality.fromByte(0))
    	  a2c(r, k.tail, s.tail) 
    	}
    }
  }

  /**
   * recursive function callee implicit
   */
  implicit def append2Composite[R <: Composite, K <: HList, S <: HList](r: R)(k: K, s: S)
  	(implicit a2c: Append2Composite[R, K, S]) = a2c(r, k, s) 
    
  /**
   * some ordering magic for HLists taken from shapeless.
   * This ordering is according to the ordering of the parts of the HList
   */
  implicit def hnilOrdering : Ordering[HNil] = new Ordering[HNil] {
    def compare(a : HNil, b : HNil) = 0
  }

  implicit def hlistOrdering[H, T <: HList](implicit oh : Ordering[H], ot : Ordering[T]) : Ordering[H :: T] = new Ordering[H :: T] {
   	def compare(a : H :: T, b : H :: T) = {
   	  val i = oh.compare(a.head, b.head)
      if (i == 0) ot.compare(a.tail, b.tail) else i
  	}
  }

}

/**
 * Hint: all implicits can be usually be provided by just static-importing 
 * the companion object and the appropriate package objects of shapeless. They are 
 * used to provide evidence for type-level (compiler-level) safety of types.
 * 
 * V must be contained in type class CassandraSerializable (see implicits because
 * context bounds and implicits do not work together in 2.9.3)
 */
class CassandraCompositeStore[RK <: HList, CK <: HList, V, RS <: HList, CS <: HList](
  val keyspace: CassandraConfiguration.StoreKeyspace,
  val columnFamily: CassandraConfiguration.StoreColumnFamily,
  val keySerializer: RS,
  val colSerializer: CS,
  val valueSerializer: Serializer[V],
  val policy: ConsistencyLevelPolicy = CassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  val poolSize: Int = CassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  val ttl: Option[Duration] = CassandraConfiguration.DEFAULT_TTL_DURATION)(
    implicit evrow: MappedAux[RK, CassandraSerializable, RS],
    evcol: MappedAux[CK, CassandraSerializable, CS], 
    a2cRow: CassandraCompositeStore.Append2Composite[Composite, RK, RS], 
    a2cCol: CassandraCompositeStore.Append2Composite[Composite, CK, CS],
    flRow: FromTraversable[RK],
    flCol: FromTraversable[CK],
    toLRow: ToList[RS, CassandraSerializable[_]],
    toLCol: ToList[CS, CassandraSerializable[_]],
    rsUTC:  *->*[CassandraSerializable]#λ[RS],
    csUTC:  *->*[CassandraSerializable]#λ[CS],
    cassSerValue: CassandraSerializable[V])
  extends Store[(RK, CK), V] with WithPutTtl[(RK, CK), V, CassandraCompositeStore[RK, CK, V, RS, CS]] 
  with CassandraPartiallyIterableStore[RK, CK, RS, CS, V, Composite] {
  keyspace.getKeyspace.setConsistencyLevelPolicy(policy)

  import CassandraCompositeStore._
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))
  
  override def withPutTtl(ttl: Duration): CassandraCompositeStore[RK, CK, V, RS, CS] = new CassandraCompositeStore[RK, CK, V, RS, CS](keyspace,
    columnFamily, keySerializer, colSerializer, valueSerializer, policy, poolSize, Option(ttl))
  
  override def multiPut[K1 <: (RK, CK)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool {
    	val mutator = HFactory.createMutator(keyspace.getKeyspace, CompositeSerializer.get)
    	kvs.foreach{ kv =>
          // create the keys for rows and columns
    	  val ((rk, ck), valueOpt) = kv
    	  val rowKey = createKey(rk, keySerializer)
    	  val colKey = createKey(ck, colSerializer)
    	  valueOpt match {
    	    case Some(value) => {
    	      val column = HFactory.createColumn(colKey, value)
    	      ttl match { 
    	        case Some(duration) => column.setTtl(duration.inSeconds)
    	        case _ =>
    	      }
      	      mutator.addInsertion(rowKey, columnFamily.name, column)
    	    }
    	    case None => mutator.addDeletion(rowKey, columnFamily.name, colKey, CompositeSerializer.get)
    	  }
    	}
    	val res = mutator.execute()
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }
  
  /**
   * creates composite key using a recursion on the implicits of type Append2Composite 
   */
  private def createKey[K <: HList, S <: HList](keys: K, keySerializers: S)
  		(implicit a2c: Append2Composite[Composite, K, S],
  		    sUTC: *->*[CassandraSerializable]#λ[S]): Composite = {
    val result = new Composite
    append2Composite(result)(keys, keySerializers)
    result
  }

  override def get(key: (RK, CK)): Future[Option[V]] = {
    val (rk, ck) = key
    val rowKey = createKey(rk, keySerializer)
    val colKey = createKey(ck, colSerializer)

    futurePool {
      val result = HFactory.createColumnQuery(keyspace.getKeyspace, CompositeSerializer.get, CompositeSerializer.get, valueSerializer)
        .setColumnFamily(columnFamily.name)
        .setKey(rowKey)
        .setName(colKey)
        .execute()
      if (result.get == null) None else Some(result.get.getValue)
    }
  }

  // methods used to provide state information to CassandraPartiallyIterableStore
  override def createColKey(key: CK): Composite = createKey(key, colSerializer)
  override def createRowKey(key: RK): Composite = createKey(key, keySerializer)
  override def getCompositeSerializer: Serializer[Composite] = CompositeSerializer.get
  override def getValueSerializer: Serializer[V] = valueSerializer
  override def getRowResult(list: List[AbstractComposite#Component[_]]): RK = compositeToList(list, 0, true).toHList[RK].getOrElse(HNil.asInstanceOf[RK])
  override def getColResult(list: List[AbstractComposite#Component[_]]): CK = compositeToList(list, 0, false).toHList[CK].getOrElse(HNil.asInstanceOf[CK]) 
  override def getFuturePool: ExecutorServiceFuturePool = futurePool
  override def getKeyspace: Keyspace = keyspace.getKeyspace
  override def getColumnFamilyName: String = columnFamily.name
  
  private def compositeToList(k: List[AbstractComposite#Component[_]], count: Int, isRowKey: Boolean): List[Any] = {
    val serializers = if (isRowKey) keySerializer.toList else colSerializer.toList
    if(k.nonEmpty)
      k.head.getValue(serializers.lift(count).get.getSerializer) :: compositeToList(k.tail, count + 1, isRowKey)
    else 
      Nil
  }

}

