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
package com.twitter.storehaus.cascading

import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.cascading.split.{ StorehausSplittingMechanism, JobConfKeyArraySplittingMechanism }
import com.twitter.util.Future
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{
  JobConf,
  InputFormat,
  InputSplit,
  RecordReader,
  Reporter
}
import java.io.{ DataInput, DataOutput }
import com.twitter.concurrent.Spool
import com.twitter.util.Try
import scala.reflect.runtime._

/**
 * Hadoop-InputFormat for Storehaus.
 * 
 * @author Ruban Monu, Andreas Petter
 */
class StorehausInputFormat[K, V, U <: AbstractStorehausCascadingInitializer]
  extends InputFormat[Instance[K], Instance[V]] {  

  /**
   * init input format by reading JobConf and provide a StorehausSplittingMechanism
   */  
  private def getSplittingMechanism(conf: JobConf): StorehausSplittingMechanism[K, V, U] = {
    StorehausInputFormat.getSplittingClass(conf).get.asInstanceOf[StorehausSplittingMechanism[K, V, U]]
  }
  
  /**
   * RecordReader delegating real work to the provided SplittingMechanism
   */
  class StorehausRecordReader(split: InputSplit, splittingMechanism: StorehausSplittingMechanism[K, V, U], reporter: Reporter)
    extends RecordReader[Instance[K], Instance[V]] {
    splittingMechanism.initializeSplitInCluster(split, reporter)
    private [this] var pos : Long = 1L

    override def next(key: Instance[K], value: Instance[V]) : Boolean = {
      pos += 1
      splittingMechanism.fillRecord(split, key, value)
    }

    override def close = splittingMechanism.closeSplit(split)
    override def createKey: Instance[K] = new Instance[K]
    override def createValue: Instance[V] = new Instance[V]
    override def getPos: Long = pos
    override def getProgress: Float = split.getLength / getPos
  }

  /**
   * returns the splits by delegating to a StorehausSplittingMechanism from JobConf
   */
  override def getSplits(conf: JobConf, hint: Int) : Array[InputSplit] = {
    getSplittingMechanism(conf).getSplits(conf, hint)
  }
 
  /**
   * returns RecordReader by providing a StorehausSplittingMechanism from JobConf
   */
  override def getRecordReader(inputSplit: InputSplit, conf: JobConf, reporter: Reporter) = {
    StorehausInputFormat.getResourceConfClass(conf).get.configure(conf)
    new StorehausRecordReader(inputSplit, getSplittingMechanism(conf), reporter)
  }
}

/**
 * convenience methods to setup input format with JobConf, which allows to configure 
 * different mechanisms for splitting in the storehaus-stores
 */
object StorehausInputFormat {
  val SPLITTING_CLASSNAME_CONFID = "com.twitter.storehaus.cascading.splitting.mechanism.class"
  
  def setSplittingClass[K, V, U <: AbstractStorehausCascadingInitializer, T <: StorehausSplittingMechanism[K, V, U]](conf: JobConf, mechanism: Class[T]) = {
    conf.set(SPLITTING_CLASSNAME_CONFID, mechanism.getName)
  } 
  
  private [storehaus] def getConfClass[T](conf: JobConf, confOption: String, createDefault: () => T): Try[T] = {
    // use reflection to initialize the class, which reads it's params from the JobConf
    val optname = Option(conf.get(confOption))
    optname match {
      case Some(name) => Try {
        val rm = universe.runtimeMirror(getClass.getClassLoader)
        val clazz = Class.forName(name)
        val clazzSymbol = rm.classSymbol(clazz)
        val refClass = rm.reflectClass(clazzSymbol)
        val constrSymbol = clazzSymbol.typeSignature.member(universe.nme.CONSTRUCTOR).asMethod
        val refConstr = refClass.reflectConstructor(constrSymbol)
        if(constrSymbol.paramss.size == 0 || constrSymbol.paramss(0).size == 0) {
          // we create an object with zero constructor parameters
          refConstr().asInstanceOf[T]
        } else {
          // we create an object with JobConf as constructor parameters (other options are not allowed)
          refConstr(conf).asInstanceOf[T]
        }
      }
      case None => Try(createDefault())
    }
  }
  
  def getSplittingClass[K, V, U <: AbstractStorehausCascadingInitializer](conf: JobConf): Try[StorehausSplittingMechanism[K, V, U]] = 
    getConfClass[StorehausSplittingMechanism[K, V, U]](conf, SPLITTING_CLASSNAME_CONFID, () => new JobConfKeyArraySplittingMechanism[K, V, U](conf))
  
  val RESOURCECONF_CLASSNAME_CONFID = "com.twitter.storehaus.cascading.splitting.resourceconf.class"
  
  def setResourceConfClass[T <: ResourceConf](conf: JobConf, resourceConf: Class[T]) = {
    conf.set(RESOURCECONF_CLASSNAME_CONFID, resourceConf.getName)
  } 
  
  def getResourceConfClass(conf: JobConf): Try[ResourceConf] =
    getConfClass[ResourceConf](conf, RESOURCECONF_CLASSNAME_CONFID, () => NullResourceConf)
  
  /**
   * used to initialize map-side resources
   */
  trait ResourceConf {
    def configure(conf: JobConf)
  }
  
  object NullResourceConf extends ResourceConf {
    override def configure(conf: JobConf) = {}
  }

}
