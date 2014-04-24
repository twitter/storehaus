package com.twitter.storehaus.cassandra

import com.twitter.util.Future

/**
 * for stores that support iterating over a subset of keys
 * TODO: maybe move this to a more general location and 
 * implement this for more stores? 
 */
trait PartiallyIterableStore[K, V] {
  
  /**
   * gets a list of entries from the store starting at key start up to end
   */
  def multiGet(start: K, end: Option[K] = None, maxNumKeys: Option[Int] = None, descending: Boolean = false): Future[Iterable[(K, V)]] 
}

