package com.twitter.storehaus.cascading.split

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{ QueryableStore, ReadableStore, ReadableStoreProxy }
import com.twitter.util.{ Await, Future }
import com.twitter.concurrent.Spool.{*::}
import org.apache.hadoop.io.Writable

class SplittableQueryableStore[K, V, Q <: Writable, T <: SplittableQueryableStore[K, V, Q, T, U], U <: QueryableStore[Q, (K, V)] with ReadableStore[K, V]]
		(store: U, splittingFunction: (Q, Int) => Seq[Q], val query: Q) 
    extends SplittableStore[K, V, Q, T] 
    with QueryableStore[Q, (K, V)] 
    with ReadableStoreProxy[K, V] {

  override def self = store

  override def getSplits(numberOfSplitsHint: Int): Seq[T] = {
    val queries = splittingFunction(query, numberOfSplitsHint)
    queries.map(qu => getSplit(qu))
  }

  override def getSplit(predicate: Q): T = {
    new SplittableQueryableStore[K, V, Q, T, U](store, splittingFunction, predicate).asInstanceOf[T]
  }

  override def getAll: Spool[(K, V)] = {
    val seq = Await.result(queryable.get(query)).getOrElse(Seq[(K, V)]())
    // TODO: make this lazy spooling
    seq.foldRight(Spool.empty[(K, V)])((kv, spool) => ((kv)) **:: spool)
  }

  override def queryable: ReadableStore[Q, Seq[(K, V)]] = store.queryable
  
  override def getInputSplits(stores: Seq[T], tapid: String): Array[SplittableStoreInputSplit[K, V, Q]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, Q](tapid, sto.query)).toArray
}