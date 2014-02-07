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

/**
 * @author Ruban Monu
 */
class StorehausInputFormat[K, V](store : ReadableStore[K, V], ks : Set[K])
  extends InputFormat[KVWrapper[K, V], NullWritable] {

  class StorehausInputSplit(ks : Set[K]) extends InputSplit {
    // obligatory interface methods
    def getLocations : Array[String] = Array.empty[String]
    def getLength : Long = 0L
    def readFields(input : DataInput) : Unit = ()
    def write(output : DataOutput) : Unit = ()
    def getKeySet: Set[K] = ks
  }

  class StorehausRecordReader(store : ReadableStore[K, V], split : StorehausInputSplit)
    extends RecordReader[KVWrapper[K, V], NullWritable] {

    private [this] val results : Iterator[(K, Future[Option[V]])] =
      store.multiGet(split.getKeySet).iterator

    private [this] var pos : Long = 0L

    def next(wrapper : KVWrapper[K, V], n : NullWritable) : Boolean =
      results.hasNext match {
        case false => false
        case true => wrapper.tuple = results.next; pos += 1; true
      }

    def close : Unit = () // TODO: check if we need to let go of any resources here

    def createKey : KVWrapper[K, V] = new KVWrapper[K, V]()

    def createValue : NullWritable = NullWritable.get

    def getPos : Long = pos

    def getProgress : Float = pos / split.getKeySet.size.toFloat
  }

  // TODO: add logging

  // TODO: add support for splitting the keyset across mappers
  def getSplits(job : JobConf, i : Int) : Array[InputSplit] = Array(new StorehausInputSplit(ks))

  def getRecordReader(inputSplit : InputSplit, job : JobConf, reporter : Reporter) =
    new StorehausRecordReader(store, inputSplit.asInstanceOf[StorehausInputSplit])
}

class KVWrapper[K, V] {
  var tuple : (K, Future[Option[V]]) = null
  // TODO: sort this out. we probably don't need this wrapper
}
