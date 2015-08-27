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
package com.twitter.storehaus.cascading

import com.twitter.storehaus.MapStore
import com.twitter.storehaus.cascading.split.SplittableStore
import com.twitter.storehaus.cascading.split.SplittableIterableStore
import com.twitter.storehaus.cascading.split.SplittableStoreCascadingInitializer
import com.twitter.storehaus.cascading.split.SplittableStoreSplittingMechanism
import org.apache.hadoop.io.LongWritable


package object examples {

  type MapStoreClassSplittableIterableStore = SplittableIterableStore[String, String, MapStore[String, String]]
  type MapStoreClassSplittableStore = SplittableStore[String, String, LongWritable]
  type MapStoreClassInitializer = SplittableStoreCascadingInitializer[String, String, LongWritable, MapStoreClassSplittableStore]
  type MapStoreClassMechanism = SplittableStoreSplittingMechanism[String, String, LongWritable, MapStoreClassSplittableStore, StoreInitializerMapStoreClass]
}