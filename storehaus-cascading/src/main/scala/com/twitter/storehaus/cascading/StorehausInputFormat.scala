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


/**
 * Hadoop-InputFormat for Storehaus.
 * 
 * @author Ruban Monu, Andreas Petter
 */
class StorehausInputFormat[K, V]
  extends InputFormat[Instance[K], Instance[V]] {  
  
  /**
   * init input format by reading JobConf and provide a StorehausSplittingMechanism
   */  
  private def getSplittingMechanism(conf: JobConf): StorehausSplittingMechanism[K, V] = {
    StorehausInputFormat.getSplittingClass(conf).get.asInstanceOf[StorehausSplittingMechanism[K, V]]
  }
  
  /**
   * RecordReader delegating real work to the provided SplittingMechanism
   */
  class StorehausRecordReader(split: InputSplit, splittingMechanism: StorehausSplittingMechanism[K, V])
    extends RecordReader[Instance[K], Instance[V]] {

    private [this] var pos : Long = 1L

    override def next(key: Instance[K], value: Instance[V]) : Boolean = {
      pos += 1
      splittingMechanism.fillRecord(split, key, value)
    }

    override def close = splittingMechanism.close 
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
    new StorehausRecordReader(inputSplit, getSplittingMechanism(conf))
  }
}

/**
 * convenience methods to setup input format with JobConf
 */
object StorehausInputFormat {
  val SPLITTINGCLASSNAMECONFID = "com.twitter.storehaus.cascading.splitting.mechanism.class"
  def setSplittingClass[K, V](conf: JobConf, mechanism: StorehausSplittingMechanism[K, V]) = {
    conf.set(SPLITTINGCLASSNAMECONFID, mechanism.getClass().getName())
  } 
  def getSplittingClass[K, V](conf: JobConf): Try[StorehausSplittingMechanism[K, V]] = {
    // use reflection to initialize the splitting class, which reads it's params from the JobConf
    val optname = Option(conf.get(SPLITTINGCLASSNAMECONFID))
    optname match {
      case Some(name) => Try { 
        val clazz = Class.forName(name).asInstanceOf[Class[StorehausSplittingMechanism[K, V]]]
        val constructor = clazz.getConstructor(classOf[JobConf])
        constructor.newInstance(Array[Object](conf))
      }
      case None => Try(new JobConfKeyArraySplittingMechanism(conf))
    }
  }
}


