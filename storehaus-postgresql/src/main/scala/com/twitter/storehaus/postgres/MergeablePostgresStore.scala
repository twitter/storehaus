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
class MergeablePostgresStore[K, V1, V2](underlying: PostgresStore[K, V1])
                                  (implicit inj: Injection[V2, V1],
                                   override val semigroup: Semigroup[V2])
extends ConvertedStore[K, K, V1, V2](underlying)(identity)
with MergeableStore[K, V2] {

  private val toV2: V1 => V2 = inj.invert(_) match {
    case Success(v) => v
    case Failure(e) => throw IllegalConversionException(e.getMessage)
  }

  /** merge a set of keys. */
  override def multiMerge[K1 <: K](kvs: Map[K1, V2]): Map[K1, Future[Option[V2]]] = {
    val result = doUpdate(kvs)(underlying.client)
    kvs.keySet.iterator.map { key =>
      (key, result.map(_.get(key).map(toV2)))
    }.toMap
  }

  protected[postgres] def doUpdate[K1 <: K](kvs: Map[K1, V2]): DBTxRequest[Map[K, V1]] = {
    _.inTransaction(implicit cli =>
      for{ existingKeys <- underlying.doGet(kvs.keySet, forUpdate = true) // SELLECT FOR UPDATE
                      // INSERT keys, fail if some keys already in the table
                      _ <- underlying.doInsert(mergeWithSemigroup(existingKeys.mapValues(toV2), kvs)
                                                 .mapValues(inj)
                                                 .toList)
      } yield existingKeys
    )
  }

  /**
   * Merge two maps(merging by the key).
   * Method will merge all values from m1 into m2.
   * All values with the same key will be merged with Semigroup[V2].plus method
   */
  private def mergeWithSemigroup[K1 <: K](m1: Map[K, V2], m2:  Map[K1, V2]): Map[K1, V2] = {
    m2.keySet.iterator.map{ k2 =>
      (k2, m1.get(k2) match {
        case Some(v1) => semigroup.plus(m2(k2), v1)
        case None => m2(k2)
      })
    }.toMap
  }

}
