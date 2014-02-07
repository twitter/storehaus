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

import cascading.flow.FlowProcess
import cascading.tap.SourceTap
import cascading.tuple.{ Fields, Tuple, TupleEntryIterator }
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{ JobConf, RecordReader }

import java.util.UUID

/**
 * @author Ruban Monu
 */
class StorehausSourceTap[K, V](store : ReadableStore[K, V],
  keySet : Set[K], fields : Fields)(implicit fn: ((K, Future[Option[V]])) => Tuple)
  extends SourceTap[JobConf, RecordReader[KVWrapper[K, V], NullWritable]](
    new StorehausSourceScheme(store, keySet, fields, UUID.randomUUID.toString)) {

  // TODO: perhaps have a separate tap for IterableStores

  private val id : String = getScheme.asInstanceOf[StorehausSourceScheme[K, V]].getId

  override def getIdentifier : String = this.id

  override def getModifiedTime(conf : JobConf) : Long = System.currentTimeMillis

  override def openForRead(process : FlowProcess[JobConf],
    input : RecordReader[KVWrapper[K, V], NullWritable]) : TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(process, this, input)

  override def resourceExists(conf : JobConf) : Boolean = true

  override def equals(o : Any) : Boolean =
    o.getClass.equals(this) && id.equals(o.asInstanceOf[StorehausSourceTap[K, V]].id)
}
