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
package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.cascading.{AbstractStorehausCascadingInitializer, StorehausCascadingInitializer}
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Writable

trait AbstractSplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q]]
  extends AbstractStorehausCascadingInitializer

trait SplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q]] 
  extends StorehausCascadingInitializer[K, V] with AbstractSplittableStoreCascadingInitializer[K, V, Q, T] {
  def getSplittableStore(jobConf: JobConf): Option[SplittableStore[K, V, Q]] 
}

trait VersionedSplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q]] 
  extends VersionedStorehausCascadingInitializer[K, V] with AbstractSplittableStoreCascadingInitializer[K, V, Q, T] {
  def getSplittableStore(jobConf: JobConf, version: Long): Option[SplittableStore[K, V, Q]] 
}
