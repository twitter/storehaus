package com.twitter.storehaus.cascading.split

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{ QueryableStore, ReadableStore, ReadableStoreProxy }
import com.twitter.util.{ Await, Future }
import com.twitter.concurrent.Spool.{*::}
import org.apache.hadoop.io.Writable

class SplittableQueryableStore[K, V, Q <: Writable, U <: QueryableStore[Q, (K, V)] with ReadableStore[K, V]]
		(store: U, splittingFunction: (Q, Int, Option[Long]) => Seq[Q], val query: Q, val version: Option[Long] = None) 
    extends SplittableStore[K, V, Q, SplittableQueryableStore[K, V, Q, U]] 
    with QueryableStore[Q, (K, V)] 
    with ReadableStoreProxy[K, V] {

  override def self = store

  override def getSplits(numberOfSplitsHint: Int): Seq[SplittableQueryableStore[K, V, Q, U]] = {
    val queries = splittingFunction(query, numberOfSplitsHint, version)
    queries.map(qu => getSplit(qu, version))
  }

  override def getSplit(predicate: Q, version: Option[Long]): SplittableQueryableStore[K, V, Q, U] = {
    new SplittableQueryableStore[K, V, Q, U](store, splittingFunction, predicate, version).asInstanceOf[SplittableQueryableStore[K, V, Q, U]]
  }

  override def getAll: Spool[(K, V)] = {
    val seq = Await.result(queryable.get(query)).getOrElse(Seq[(K, V)]())
    // TODO: make this lazy spooling
    seq.foldRight(Spool.empty[(K, V)])((kv, spool) => ((kv)) **:: spool)
  }

  override def queryable: ReadableStore[Q, Seq[(K, V)]] = store.queryable
  
  override def getInputSplits(stores: Seq[SplittableQueryableStore[K, V, Q, U]], tapid: String, version: Option[Long]): Array[SplittableStoreInputSplit[K, V, Q]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, Q](tapid, sto.query, version)).toArray
}