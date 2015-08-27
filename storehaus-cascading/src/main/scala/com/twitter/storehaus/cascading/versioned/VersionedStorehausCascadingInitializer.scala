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
package com.twitter.storehaus.cascading.versioned

import org.apache.hadoop.mapred.JobConf
import com.twitter.util.Closable
import com.twitter.storehaus.{ReadableStore, WritableStore}
import com.twitter.storehaus.cascading.AbstractStorehausCascadingInitializer

/**
 * This initializer trait is implemented by cascading
 * map/reduce applications / workflows using an object.
 * Being a static context it will be intitialized in every
 * virtual machine provided by Hadoop. This allows using 
 * type safe constructs, using scala-type aliases.
 * Implemenetors may not depend on any state information
 * other than JobConf.
 */
trait VersionedStorehausCascadingInitializer[K, V] extends AbstractStorehausCascadingInitializer {

  /**
   *  is executed once and only on client side
   */ 
  def prepareStore(version: Long): Boolean
  
  /**
   * returns an intialized readableStore, executed on cluster machines
   */
  def getReadableStore(jobConf: JobConf, version: Long): Option[ReadableStore[K, V]] 
  
  /** 
   * returns an initialized writableStore, executed on cluster machines
   */
  def getWritableStore(jobConf: JobConf, version: Long): Option[WritableStore[K, Option[V]]]
  
}