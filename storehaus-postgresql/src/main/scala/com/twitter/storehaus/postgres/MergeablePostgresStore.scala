package com.twitter.storehaus.postgres

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.finagle.postgres.Client
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future

import scala.util.{Failure, Success}

/**
  * @author Alexey Ponkin
  */
class MergeablePostgresStore[K, V](underlying: PostgresStore[K, V])
                                  (implicit override val semigroup: Semigroup[V])
extends MergeableStore[K, V] {

  /**
   * Delegate other ooperations to underlying store
   */
  override def get(key: K): Future[Option[V]] = underlying.get(key)
  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = underlying.multiGet(ks)
  override def put(kv: (K, Option[V])): Future[Unit] = underlying.put(kv)
  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = underlying.multiPut(kvs)

  /** merge a set of keys. */
  override def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = {
    val result = doUpdate(kvs)(underlying.client)
    kvs.keySet.iterator.map { key =>
      (key, result.map(_.get(key)))
    }.toMap
  }

  /**
   * Select keys from store, merging existing values with Semigroup
   * non-existing keys are inserted as is.
   * NOTE: Here we are using simple INSERT not UPSERT. During this
   * transaction non existent keys can be inserted in another transaction, 
   * if so, method will fail, and client must repeat request with the same arguments.
   */
  protected[postgres] def doUpdate[K1 <: K](kvs: Map[K1, V]): DBTxRequest[Map[K, V]] = {
    _.inTransaction(implicit cli =>
      for{ existingKeys <- underlying.doGet(kvs.keySet, forUpdate = true) // lock selected rows
           toUpdate   = kvs.filterKeys( existingKeys.contains(_) ) // must be merged withSemigroup
           toInsert   = kvs.filterKeys( !existingKeys.contains(_) ) // must be inserted as is
                      _ <- underlying.doUpsert(
                                               mergeWithSemigroup(
                                                                  existingKeys, 
                                                                  toUpdate
                                                                ).toList)
                      _ <- underlying.doInsert(toInsert.toList)
      } yield existingKeys
    )
  }

  /**
   * Merge two maps(merging by the key).
   * Method will merge all values from m1 into m2.
   * All values with the same key will be merged with Semigroup[V2].plus method
   */
  private def mergeWithSemigroup[K1 <: K](m1: Map[K, V], m2:  Map[K1, V]): Map[K1, V] = {
    m2.keySet.iterator.map{ k2 =>
      (k2, m1.get(k2) match {
        case Some(v1) => semigroup.plus(m2(k2), v1)
        case None => m2(k2)
      })
    }.toMap
  }
}
