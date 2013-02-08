package com.twitter.storehaus

import com.twitter.util.Future

import Store.{selectFirstSuccessfulTrial => selectFirst}

/**
 * Replicates writes to all stores, and takes the first successful read.
 */
class ReplicatedStore[StoreType <: Store[StoreType, K, V], K, V](stores: Seq[StoreType])
    extends Store[ReplicatedStore[StoreType, K, V], K, V] {
  override def get(k: K) = selectFirst(stores.map { _.get(k) })
  override def multiGet(ks: Set[K]) = selectFirst(stores.map { _.multiGet(ks) })
  override def update(k: K)(fn: Option[V] => Option[V]) =
    Future.collect(stores.map { _.update(k)(fn) }).map { new ReplicatedStore(_) }
  override def -(k: K) =
    Future.collect(stores.map { _ - k }).map { new ReplicatedStore(_) }
  override def +(pair: (K,V)) =
    Future.collect(stores.map { _ + pair }).map { new ReplicatedStore(_) }
}