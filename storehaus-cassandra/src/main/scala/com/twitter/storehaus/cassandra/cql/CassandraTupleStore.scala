package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.Row
import com.twitter.storehaus.Store
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.Future
import shapeless.Tuples._
import shapeless._

/**
 * AbstractCQLCassandraCompositeStore-wrapper for Tuples to HList. This makes it 
 * easy to use this store with tuples, which can in turn be serialized easily.
 */
class CassandraTupleStore[RKT <: Product, CKT <: Product, V, RK <: HList, CK <: HList, RS <: HList, CS <: HList]
		(store: AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS], paramToPreventWritingDownTypes: (RKT, CKT))
		(implicit ev1: HListerAux[RKT, RK],
		    ev2: HListerAux[CKT, CK],
		    ev3: TuplerAux[RK, RKT],
		    ev4: TuplerAux[CK, CKT])
	extends Store[(RKT, CKT), V]  
    with CassandraCascadingRowMatcher[(RKT, CKT), V] {

  override def get(k: (RKT, CKT)): Future[Option[V]] = {
    store.get((k._1.hlisted, k._2.hlisted))
  }
  
  override def multiPut[K1 <: (RKT, CKT)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    val resultMap = store.multiPut(kvs.map(kv => ((kv._1._1.hlisted, kv._1._2.hlisted), kv._2)))
    resultMap.map(kv => ((kv._1._1.tupled, kv._1._2.tupled).asInstanceOf[K1], kv._2))
  }
  
  override def getKeyValueFromRow(row: Row): ((RKT, CKT), V) = {
    val (keys, value) = store.getKeyValueFromRow(row)
    ((keys._1.tupled, keys._2.tupled), value)
  }
  
  override def getColumnNamesString: String = store.getColumnNamesString
}