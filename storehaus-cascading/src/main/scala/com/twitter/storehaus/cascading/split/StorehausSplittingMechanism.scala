/*
 * Copyright 2014 Twitter Inc.
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
package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.cascading.{AbstractStorehausCascadingInitializer, Instance}
import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }

/**
 * Mechanisms to split Storehaus read operations and distributes 
 * information (like keys) in the cluster
 * 
 * @author AndreasPetter
 */
abstract class StorehausSplittingMechanism[K, V, U <: AbstractStorehausCascadingInitializer](val conf: JobConf) {
  def getSplits(job: JobConf, hint: Int) : Array[InputSplit]
  
  /**
   * before we read from the split this method will be called to initialize resources.
   * For a single Cluster machine it is guarenteed that the split is the same 
   * as in fillRecord.
   */
  def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {}
  
  /**
   * similar to InputSplit.next
   */
  def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean
  
  /**
   * free resources after splitting is done
   */
  def close: Unit = {}
  
  /**
   * free resources of split
   */
  def closeSplit(split: InputSplit): Unit = {}
}


