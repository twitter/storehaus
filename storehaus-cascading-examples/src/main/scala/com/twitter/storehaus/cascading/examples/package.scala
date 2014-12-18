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