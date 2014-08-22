package com.twitter.storehaus.cassandra.cql.cascading

/**
 * This needs to be provided only in case of 
 * using Cassandra as a ReadableStore with Cascading 
 */
trait CassandraCascadingInitializer[K, V] {

  /**
   * comma separated list of connection strings
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
   * Name of the partitioner used in cassandra.yaml 
   * See package org.apache.cassandra.dht.*Partitioner for options
   */
  def getPartitionerName: String = "org.apache.cassandra.dht.Murmur3Partitioner"

}