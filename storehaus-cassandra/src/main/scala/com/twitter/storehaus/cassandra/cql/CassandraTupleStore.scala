package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.Row
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, ReadableStore, QueryableStore, Store}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Future, FutureTransformer, Return, Throw}
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
    with CassandraCascadingRowMatcher[(RKT, CKT), V] 
    with QueryableStore[String, ((RKT, CKT), V)]
    with IterableStore[(RKT, CKT), V] {

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
  
  override def queryable: ReadableStore[String, Seq[((RKT, CKT), V)]] = new Object with ReadableStore[String, Seq[((RKT, CKT), V)]] {
    override def get(whereCondition: String): Future[Option[Seq[((RKT, CKT), V)]]] = store.queryable.get(whereCondition).transformedBy {
      new FutureTransformer[Option[Seq[((RK, CK), V)]], Option[Seq[((RKT, CKT), V)]]] {
        override def map(value: Option[Seq[((RK, CK), V)]]): Option[Seq[((RKT, CKT), V)]] = value match {
          case Some(seq) => Some(seq.view.map(res => ((res._1._1.tupled, res._1._2.tupled).asInstanceOf[(RKT, CKT)], res._2)))
          case _ => None
        }
      }
    }
  }
  
  /**
   * allows iteration over all (K, V) using a query against queryable
   */
  override def getAll: Future[Spool[((RKT, CKT), V)]] = queryable.get("").transform {
    case Throw(y) => Future.exception(y)
    case Return(x) => IterableStore.iteratorToSpool(x.getOrElse(Seq[((RKT, CKT), V)]()).view.iterator)   
  }
}
