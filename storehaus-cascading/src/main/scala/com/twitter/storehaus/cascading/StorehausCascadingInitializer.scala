package com.twitter.storehaus.cascading

import org.apache.hadoop.mapred.JobConf
import com.twitter.util.Closable
import com.twitter.storehaus.{ReadableStore, WritableStore}

/**
 * This initializer trait is implemented by cascading
 * map/reduce applications / workflows using an object.
 * Being a static context it will be intitialized in every
 * virtual machine provided by Hadoop. This allows using 
 * type safe constructs, using scala-type aliases.
 * Implemenetors may not depend on any state information
 * other than JobConf.
 */
trait StorehausCascadingInitializer[K, V] {

  /**
   *  is executed once and only on client side
   */ 
  def prepareStore: Boolean
  
  /**
   * returns an intialized readableStore, executed on cluster machines
   */
  def getReadableStore(jobConf: JobConf): Option[ReadableStore[K, V]] 
  
  /** 
   * returns an initialized writableStore, executed on cluster machines
   */
  def getWritableStore(jobConf: JobConf): Option[WritableStore[K, Option[V]]]
  
}