package com.twitter.storehaus.cassandra.cql.cascading

/**
 * This needs to be provided only in case of 
 * using Cassandra as a ReadableStore with Cascading, i.e. 
 * required only for using CassandraSplittingMechanism
 */
trait CassandraCascadingInitializer[K, V] {

  /**
   * thrift connection to Cassandra host:port
   */
  def getThriftConnections: String
  
  /**
   * name of the ColumnFamily
   */
  def getColumnFamilyName: String
  
  /**
   * Name of the keyspace in which the ColumnFamily resides in
   */
  def getKeyspaceName: String
  
  /**
   * return a store that is an implementation of CassandraCascadingRowMatcher
   */
  def getCascadingRowMatcher: CassandraCascadingRowMatcher[K, V]
  
  /**
   * fill if any where clauses should prevent fetching all rows
   */
  def getUserDefinedWhereClauses: String = ""

  /**
   * Set of Strings host:port to native protocol port
   */
  def getNativePort: Int = 9042
  
  /**
   * Name of the partitioner used in cassandra.yaml 
   * See package org.apache.cassandra.dht.*Partitioner for options
   */
  def getPartitionerName: String = "org.apache.cassandra.dht.Murmur3Partitioner"
  
}