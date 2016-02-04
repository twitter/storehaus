
package com.twitter.storehaus.algebra

import com.twitter.algebird.Semigroup
import com.twitter.util.{Closable, Future, Time}
import java.util.concurrent.{ ConcurrentHashMap => JConcurrentHashMap }

object ConcurrentHashMapMergeableStore {
  def apply[K, V: Semigroup](): ConcurrentHashMapMergeableStore[K, V] =
    new ConcurrentHashMapMergeableStore[K, V](new JConcurrentHashMap[K, V])
}

/** A threadsafe local MergeableStore
 *  useful for testing or other local applications
 */
class ConcurrentHashMapMergeableStore[K, V](map: JConcurrentHashMap[K, V])(
  implicit val semigroup: Semigroup[V]) extends MergeableStore[K, V] {

  override def get(k: K): Future[Option[V]] = Future.value(Option(map.get(k)))
  override def put(kv: (K, Option[V])): Future[Unit] = kv match {
    case (k, Some(v)) => map.put(k, v); Future.Unit
    case (k, None) => map.remove(k); Future.Unit
  }

  override def merge(kv: (K, V)): Future[Option[V]] = {
    val (k, v) = kv
    map.get(k) match {
      case null =>
        if (null == map.putIfAbsent(k, v)) Future.None
        else merge(kv)
      case oldV =>
        if (map.replace(k, oldV, semigroup.plus(oldV, v))) Future.value(Some(oldV))
        else merge(kv)
    }
  }
}
