/*
 * Copyright 2014 Twitter, Inc.
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

import com.datastax.driver.core.Row
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, ReadableStore, QueryableStore, Store}
import com.twitter.util.{Future, FutureTransformer}
import shapeless._
import shapeless.ops.hlist.Tupler
import shapeless.syntax.std.tuple._

/**
 * AbstractCQLCassandraCompositeStore-wrapper for Tuples to HList. This makes it 
 * easy to use this store with tuples, which can in turn be serialized easily.
 */
class CassandraTupleStore[RKT <: Product, CKT <: Product, V, RK <: HList, CK <: HList, RS <: HList, CS <: HList]
		(val store: AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS], paramToPreventWritingDownTypes: (RKT, CKT))
		(implicit ev1: Generic.Aux[RKT, RK],
		    ev2: Generic.Aux[CKT, CK])
	extends Store[(RKT, CKT), V]  
    with QueryableStore[String, ((RKT, CKT), V)]
    with IterableStore[(RKT, CKT), V] {

  override def get(k: (RKT, CKT)): Future[Option[V]] = {
    store.get((ev1.to(k._1), ev2.to(k._2)))
  }
  
  override def multiPut[K1 <: (RKT, CKT)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    val resultMap = store.multiPut(kvs.map(kv => ((ev1.to(kv._1._1), ev2.to(kv._1._2)), kv._2)))
    resultMap.map(kv => ((ev1.from(kv._1._1), ev2.from(kv._1._2)).asInstanceOf[K1], kv._2))
  }
  
  // interim: no override
  def getKeyValueFromRow(row: Row): ((RKT, CKT), V) = {
    val (keys, value) = store.getKeyValueFromRow(row)
    ((ev1.from(keys._1), ev2.from(keys._2)), value)
  }
  
  // interim: no override
  def getColumnNamesString: String = store.getColumnNamesString
  
  override def queryable: ReadableStore[String, Seq[((RKT, CKT), V)]] = new Object with ReadableStore[String, Seq[((RKT, CKT), V)]] {
    override def get(whereCondition: String): Future[Option[Seq[((RKT, CKT), V)]]] = store.queryable.get(whereCondition).transformedBy {
      new FutureTransformer[Option[Seq[((RK, CK), V)]], Option[Seq[((RKT, CKT), V)]]] {
        override def map(value: Option[Seq[((RK, CK), V)]]): Option[Seq[((RKT, CKT), V)]] = value match {
          case Some(seq) => Some(seq.view.map(res => ((ev1.from(res._1._1), ev2.from(res._1._2)).asInstanceOf[(RKT, CKT)], res._2)))
          case _ => None
        }
      }
    }
  }
  
  override def getAll: Future[Spool[((RKT, CKT), V)]] = queryable.get("").flatMap { x =>
    IterableStore.iteratorToSpool(x.getOrElse(Seq[((RKT, CKT), V)]()).view.iterator)   
  }
}

/**
 * Muti-valued version of CassandraTupleStore
 */
class CassandraTupleMultiValueStore[RKT <: Product, CKT <: Product, V <: Product, RK <: HList, CK <: HList, VL <: HList, RS <: HList, CS <: HList, VS <: HList]
		(val store: CQLCassandraMultivalueStore[RK, CK, VL, RS, CS, VS], 
		    paramToPreventWritingDownTypes1: (RKT, CKT),
		    paramToPreventWritingDownTypes2: V)
		(implicit ev1: Generic.Aux[RKT, RK],
		    ev2: Generic.Aux[CKT, CK],
		    ev3: Generic.Aux[V, VL])
	extends Store[(RKT, CKT), V]  
    with QueryableStore[String, ((RKT, CKT), V)]
    with IterableStore[(RKT, CKT), V] {

  override def get(k: (RKT, CKT)): Future[Option[V]] = {
    store.get((ev1.to(k._1), ev2.to(k._2))).flatMap(opt => Future(opt.map(res => ev3.from(res))))
  }
  
  override def multiPut[K1 <: (RKT, CKT)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    val resultMap = store.multiPut(kvs.map(kv => ((ev1.to(kv._1._1), ev2.to(kv._1._2)), kv._2.map(v => ev3.to(v)))))
    resultMap.map(kv => ((ev1.from(kv._1._1), ev2.from(kv._1._2)).asInstanceOf[K1], kv._2))
  }
  
  // interim: no override
  def getKeyValueFromRow(row: Row): ((RKT, CKT), V) = {
    val (keys, value) = store.getKeyValueFromRow(row)
    ((ev1.from(keys._1), ev2.from(keys._2)), ev3.from(value))
  }
  
  // interim: no override
  def getColumnNamesString: String = store.getColumnNamesString
  
  override def queryable: ReadableStore[String, Seq[((RKT, CKT), V)]] = new Object with ReadableStore[String, Seq[((RKT, CKT), V)]] {
    override def get(whereCondition: String): Future[Option[Seq[((RKT, CKT), V)]]] = store.queryable.get(whereCondition).transformedBy {
      new FutureTransformer[Option[Seq[((RK, CK), VL)]], Option[Seq[((RKT, CKT), V)]]] {
        override def map(value: Option[Seq[((RK, CK), VL)]]): Option[Seq[((RKT, CKT), V)]] = value match {
          case Some(seq) => Some(seq.view.map(res => ((ev1.from(res._1._1), ev2.from(res._1._2)), ev3.from(res._2))))
          case _ => None
        }
      }
    }
  }
  
  override def getAll: Future[Spool[((RKT, CKT), V)]] = queryable.get("").flatMap { x =>
    IterableStore.iteratorToSpool(x.getOrElse(Seq[((RKT, CKT), V)]()).view.iterator)   
  }
}

