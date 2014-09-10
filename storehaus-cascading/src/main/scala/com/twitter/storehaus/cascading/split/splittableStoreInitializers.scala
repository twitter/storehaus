package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.cascading.StorehausCascadingInitializer
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Writable

trait SplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q, T]] 
  extends StorehausCascadingInitializer[K, V] {
  def getSplittableStore(jobConf: JobConf): Option[SplittableStore[K, V, Q, T]] 
}

trait VersionedSplittableStoreCascadingInitializer[K, V, Q <: Writable, T <: SplittableStore[K, V, Q, T]] 
  extends VersionedStorehausCascadingInitializer[K, V] {
  def getSplittableStore(jobConf: JobConf, version: Long): Option[SplittableStore[K, V, Q, T]] 
}
