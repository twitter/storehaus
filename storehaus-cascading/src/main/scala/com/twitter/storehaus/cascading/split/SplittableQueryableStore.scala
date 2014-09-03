package com.twitter.storehaus.cascading.split

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{ QueryableStore, ReadableStore, ReadableStoreProxy }
import com.twitter.util.{ Await, Future }
import com.twitter.concurrent.Spool.{*::}

class SplittableQueryableStore[K, V, Q, T <: SplittableQueryableStore[K, V, Q, T, U], U <: QueryableStore[Q, K] with ReadableStore[K, V]]
		(store: U, splittingFunction: (Q, Int) => Seq[Q], query: Q) 
    extends SplittableStore[K, V, Q, T] 
    with QueryableStore[Q, K] 
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
    val seq = Await.result(queryable.get(query)).getOrElse(Seq[K]())
    // this Spool must be eager anyways...
    seq.foldRight(Spool.empty[(K, V)])((key, spool) => (key, Await.result(store.get(key)).get) **:: spool)
  }

  override def queryable: ReadableStore[Q, Seq[K]] = store.queryable
}