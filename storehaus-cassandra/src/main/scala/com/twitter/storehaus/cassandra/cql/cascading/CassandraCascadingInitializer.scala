/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.storehaus.cassandra.cql.cascading

import com.twitter.storehaus.cascading.AbstractStorehausCascadingInitializer

/**
 * This needs to be provided only in case of 
 * using Cassandra as a ReadableStore with Cascading, i.e. 
 * required only for using CassandraSplittingMechanism
 */
trait CassandraCascadingInitializer[K, V] extends AbstractStorehausCascadingInitializer {

  /**
   * thrift connection to Cassandra host:port
   */
  def getThriftConnections: String
  
  /**
   * name of the ColumnFamily.
   * If the StorehausTap was intitialized with versioning a version is provided 
   */
  def getColumnFamilyName(version: Option[Long] = None): String
  
  /**
   * Name of the keyspace in which the ColumnFamily resides in
   */
  def getKeyspaceName: String
  
  /**
   * return a store that is an implementation of CassandraCascadingRowMatcher
   */
  def getCascadingRowMatcher: CassandraCascadingRowMatcher[K, V]
  
  /**
   * if any where clauses should prevent fetching all rows.
   * If the StorehausTap was intitialized with versioning a version is provided 
   */
  def getUserDefinedWhereClauses(version: Option[Long] = None): String = ""

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