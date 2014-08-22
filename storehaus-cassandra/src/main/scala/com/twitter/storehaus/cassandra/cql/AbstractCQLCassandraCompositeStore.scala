/*
 * Copyright 2014 Twitter Inc.
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
package com.twitter.storehaus.cassandra.cql

import com.twitter.util.{Duration, Future, FuturePool, Time}
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Statement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, ReconnectionPolicy, RetryPolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Clause, QueryBuilder, Update}
import com.twitter.storehaus.Store
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.{Executors, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import shapeless._
import HList._
import Traversables._
import Nat._
import UnaryTCConstraint._

object AbstractCQLCassandraCompositeStore {
  
  /**
   * used for mapping the keys in the HLists to Strings
   */
  object keyStringMapping extends (CassandraPrimitive ~>> String) {
    override def apply[T](c: CassandraPrimitive[T]) = c.cassandraType
  }
  
  /**
   * used to map over an example key to implicit serializers
   */
  private object cassandraSerializerCreation extends Poly1 {
    implicit def default[T : CassandraPrimitive] = at[T](_ => implicitly[CassandraPrimitive[T]])
  }

  /**
   * creates an HList of serializers (CasasandraPrimitives) given an example 
   * key HList and pulls serializers out of implicit scope.
   */
  def getSerializerHListByExample[KL <: HList, SL <: HList](keys: KL)(implicit mapper: MapperAux[cassandraSerializerCreation.type, KL, SL]) : SL = {
    keys.map(cassandraSerializerCreation)
  } 

  /**
   * helper trait for declaring the HList recursive function 
   * to append keys on a Composite in a type safe way
   */
  trait Append2Composite[R <: ArrayBuffer[Clause], K <: HList] {
    def apply(r: R, k: K, s: List[String]): Unit
  }

  /**
   * helper implicits for the recursion itself
   */
  object Append2Composite {
    implicit def hnilAppend2Composite[R <: ArrayBuffer[Clause]] = new Append2Composite[R, HNil] {
      override def apply(r: R, k: HNil, s: List[String]): Unit = {}
    }
    implicit def hlistAppend2Composite[R <: ArrayBuffer[Clause], M, K <: HList](
      implicit a2c: Append2Composite[R, K]) = new Append2Composite[R, M :: K] {
    	override def apply(r: R, k: M :: K, s: List[String]): Unit = {
   	      r.add(QueryBuilder.eq(s"""\"${s.head}\"""", k.head))
    	  a2c(r, k.tail, s.tail) 
    	}
    }
  }

  /**
   * recursive function callee implicit
   */
  implicit def append2Composite[R <: ArrayBuffer[Clause], K <: HList, S <: List[String]](r: R)(k: K, s: List[String])
  	(implicit a2c: Append2Composite[R, K]) = a2c(r, k, s) 
  
  /** 
   *  provides a join method for Traversables,
   *  this is actually a fold with an initial function
   */
  class Joinable[T](val traversable: Traversable[T]) {
    def join[R](initialFunction: T => R)(inbetweenFunction: (T, R) => R): R = {
      @tailrec def recJoin(acc: Option[R], traverse: Traversable[T]): R = {
        if (traverse.size == 0) return acc.get
        println(" traverse.head:" + traverse.head)
        println(" acc:" + acc)
        // println(" initialFunction:" + initialFunction(traverse.head))
        val result = acc match {
          case Some(accResult) => Some(inbetweenFunction(traverse.head, accResult))
          case None => Some(initialFunction(traverse.head))
        }
        println("result:" + result)
        recJoin(result, traverse.tail)
      }
      recJoin(None, traversable)  
    }
  }
  implicit def convertToJoinable[T](traversable: Traversable[T]) = new Joinable[T](traversable) 
}

abstract class AbstractCQLCassandraCompositeStore[RK <: HList, CK <: HList, V, RS <: HList, CS <: HList] (
  columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
    implicit evrow: MappedAux[RK, CassandraPrimitive, RS],
    evcol: MappedAux[CK, CassandraPrimitive, CS], 
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK], 
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK],
    rsUTC:  *->*[CassandraPrimitive]#λ[RS],
    csUTC:  *->*[CassandraPrimitive]#λ[CS])
  extends Store[(RK, CK), V] {
  import AbstractCQLCassandraCompositeStore._

  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  protected def putValue(value: V, update: Update): Update.Assignments
    
  override def multiPut[K1 <: (RK, CK)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool {
    	
        val mutator = new BatchStatement(batchType)
    	kvs.foreach{ kv =>
    	  val ((rk, ck), valueOpt) = kv
    	  val eqList = new ArrayBuffer[Clause]
  	      addKey(rk, rowkeyColumnNames, eqList)
   	      addKey(ck, colkeyColumnNames, eqList)
    	  val builder: BuiltStatement = valueOpt match {
    	    case Some(value) => {
    	      val update = putValue(value, QueryBuilder.update(columnFamily.getPreparedNamed)).where(_)
    	      val where = eqList.join(update)((clause, where) => where.and(clause))
    	      ttl match {
    	        case Some(duration) => where.using(QueryBuilder.ttl(duration.inSeconds))
    	        case None => where
    	      }
    	    }
    	    case None => eqList.join(QueryBuilder.delete(valueColumnName).from(columnFamily.getPreparedNamed).where(_))((clause, where) => where.and(clause))
    	  }
    	  mutator.add(builder)
    	}
    	mutator.setConsistencyLevel(consistency)
    	val res = columnFamily.session.getSession.execute(mutator)
    	
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  /**
   * creates statement using a recursion on the implicits of type Append2Composite 
   */
  protected def addKey[K <: HList, S <: List[String]](keys: K, colNames: S, clauses: ArrayBuffer[Clause])
  		(implicit a2c: Append2Composite[ArrayBuffer[Clause], K]) = {
    append2Composite(clauses)(keys, colNames)
  }

  protected def getValue(result: ResultSet): Option[V]
  
  override def get(key: (RK, CK)): Future[Option[V]] = {
    val (rk, ck) = key
    futurePool {
      val builder = QueryBuilder
        .select()
        .from(columnFamily.getPreparedNamed)
        .where()
   	  val eqList = new ArrayBuffer[Clause]
      addKey(rk, rowkeyColumnNames, eqList)
      addKey(ck, colkeyColumnNames, eqList)
      eqList.foreach(clause => builder.and(clause))
      builder.limit(1).setConsistencyLevel(consistency)
      val result = columnFamily.session.getSession.execute(builder)
      result.isExhausted() match {
        case false => {
          getValue(result: ResultSet)
        }
        case true => None
      }
    }
  }
  
  override def close(deadline: Time) = {
    Future[Unit] {
      futurePool.executor.awaitTermination(deadline.sinceNow.inUnit(TimeUnit.SECONDS), TimeUnit.SECONDS)
    }
  }

}
