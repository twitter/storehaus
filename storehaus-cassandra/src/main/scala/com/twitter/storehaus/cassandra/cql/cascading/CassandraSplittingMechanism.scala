package com.twitter.storehaus.cassandra.cql.cascading

import org.apache.hadoop.mapred.{ InputSplit, JobConf }
import com.twitter.storehaus.cascading.InitializableStoreObjectSerializer
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.TFramedTransportFactory
import org.apache.cassandra.hadoop.{ ColumnFamilyInputFormat, ConfigHelper }
import com.twitter.util.Try
import com.twitter.storehaus.cascading.Instance
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism

/**
 * Cassandra does not directly support Splitting, so we provide 
 * a special splitting mechanism which is able to split a Cassandra
 * 
 */
class CassandraSplittingMechanism[K, V](override val conf: JobConf) 
	extends StorehausSplittingMechanism[K, V](conf: JobConf) {
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val storeinit = InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
  val JOBCONF_QUERY_STRING = "com.twitter.storehaus.cascading.split.cassandra.query." + tapid
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    val query = job.get(JOBCONF_QUERY_STRING)
    // ask for contact information -> call get_splits_ex
    val connectionOptions = storeinit.getThriftConnections.split(",").
      map(s => s.trim.split(":")).
      map(a => (a.lift(0).get, a.lift(1).get.toInt)).toList
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName)
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    val columnFamilyFormat = new ColumnFamilyInputFormat
    columnFamilyFormat.getSplits(conf, hint)
  }
  
  /**
   * similar to InputSplit.next
   */
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    true
  }
  
  /**
   * free resources after splitting is done
   */
  override def close = {}
}