/*
 * Copyright 2014 SEEBURGER AG
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