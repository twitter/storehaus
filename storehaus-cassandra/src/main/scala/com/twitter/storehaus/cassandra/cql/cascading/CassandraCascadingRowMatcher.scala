package com.twitter.storehaus.cassandra.cql.cascading

import com.datastax.driver.core.Row

/**
 * used to match Hadoop results to the corresponding format of 
 * the store
 */
trait CassandraCascadingRowMatcher[K, V] {

  def getKeyValueFromRow(row: Row): (K, V)
  
}