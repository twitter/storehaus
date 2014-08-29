package com.twitter.storehaus.cassandra.cql.cascading

import com.datastax.driver.core.Row

/**
 * used to match Hadoop results to the corresponding format of 
 * the store
 */
trait CassandraCascadingRowMatcher[K, V] {

  /**
   * return a key value tuple from a Cassandra-row
   */
  def getKeyValueFromRow(row: Row): (K, V)
  
  /**
   * return names of columns as in Cassandra
   */
  def getColumnNamesString: String
  
}