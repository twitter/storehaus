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
    val result = doUpdate(kvs)
    kvs.keySet.iterator.map { key =>
      (key, result.map(_.get(key).map(toV2)))
    }.toMap
  }

  private def doUpdate[K1 <: K](kvs: Map[K1, V2]) =
    for{ existingKeys <- underlying.doGet(kvs.keySet)(underlying.client) // select elements from store
         _ <- underlying.doUpsert(mergeWithSemigroup(existingKeys, kvs).mapValues(inj).toList)(underlying.client)
    } yield existingKeys

  private def mergeWithSemigroup[K1 <: K](m1: Map[K, V1], m2:  Map[K1, V2]):  Map[K1, V2] = {
    m2.keySet.iterator.map{ key =>
      (key, m1.get(key) match {
        case Some(v1) => semigroup.plus(m2(key), toV2(v1))
        case None => m2(key)
      })
    }.toMap
  }

}
