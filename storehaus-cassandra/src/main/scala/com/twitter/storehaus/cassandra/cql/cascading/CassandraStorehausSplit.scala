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

