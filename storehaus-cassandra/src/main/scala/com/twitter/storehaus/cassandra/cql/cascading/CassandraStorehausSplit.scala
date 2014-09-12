/*
 * Copyright 2014 SEEBURGER AG
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
package com.twitter.storehaus.cassandra.cql.cascading

import org.apache.hadoop.mapred.{ JobConf }
import org.apache.cassandra.hadoop.ColumnFamilySplit
import org.apache.cassandra.hadoop.cql3.CqlRecordReader
import org.apache.hadoop.io.Writable
import java.io.{ DataInput, DataOutput }

/**
 * split for Cassandra. Fields are required to be vars, because 
 * Hadoop requires a default Java-constructor 
 */
class CassandraStorehausSplit(var tapid: String, 
    var startToken: String, 
    var endToken: String, 
    var length: Long, 
    var dataNodes: Array[String])
  extends ColumnFamilySplit(startToken, endToken, length, dataNodes) 
  with Writable {
  var tap = tapid
  @transient var position = 0l;
  @transient var recordReader: CqlRecordReader = null;
  
  protected def this() {
    this(null, null, null, 0l, null)
  }
  
  override def write(out: DataOutput): Unit = {
	out.writeUTF(tapid)
	super.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    tap = in.readUTF()
    super.readFields(in)
  }
}

object CassandraStorehausSplit {
  def apply(tapid: String, inputSplit: ColumnFamilySplit) = {
    new CassandraStorehausSplit(tapid, inputSplit.getStartToken(), 
        inputSplit.getEndToken(), inputSplit.getLength(), inputSplit.getLocations())
  } 
}

