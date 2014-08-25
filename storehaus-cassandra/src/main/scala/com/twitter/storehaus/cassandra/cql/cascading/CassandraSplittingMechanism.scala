package com.twitter.storehaus.cassandra.cql.cascading

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.storehaus.cascading.InitializableStoreObjectSerializer
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.TFramedTransportFactory
import org.apache.cassandra.hadoop.{ AbstractColumnFamilyInputFormat, ColumnFamilyInputFormat, ColumnFamilySplit, ConfigHelper }
import com.twitter.util.Try
import com.twitter.storehaus.cascading.Instance
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import org.apache.cassandra.hadoop.cql3.CqlRecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import org.apache.cassandra.utils.FBUtilities
import scala.collection.JavaConversions._
import java.net.InetAddress

/**
 * Cassandra does not directly support Splitting, so we provide 
 * a special splitting mechanism which is able to split a Cassandra
 */
class CassandraSplittingMechanism[K, V](override val conf: JobConf) 
	extends StorehausSplittingMechanism[K, V](conf: JobConf) {
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val storeinit = InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
  val JOBCONF_QUERY_STRING = "com.twitter.storehaus.cascading.split.cassandra.query." + tapid
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    val query = job.get(JOBCONF_QUERY_STRING)
    // ask for contact information -> call get_splits_ex via ColumnFamilyInputFormat
    val connectionOptions = storeinit.getThriftConnections.trim.split(":").map(s => s.trim)
    ConfigHelper.setInputInitialAddress(conf, connectionOptions.head)
    ConfigHelper.setInputRpcPort(conf, if (connectionOptions.size == 1) "9160" else connectionOptions.last)
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName)
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    val columnFamilyFormat = new ColumnFamilyInputFormat
    columnFamilyFormat.getSplits(conf, hint).
      map(split => CassandraStorehausSplit(tapid, split.asInstanceOf[ColumnFamilySplit]))
  }
  
  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    val storesplit = getStorehausSplit(split)
    val tac = new TaskAttemptContext(conf, TaskAttemptID.forName(conf.get(AbstractColumnFamilyInputFormat.MAPRED_TASK_ID))) {
      override def progress() = reporter.progress()        
    }
    storesplit.recordReader = new CqlRecordReader
    storesplit.recordReader.initialize(split.asInstanceOf[org.apache.hadoop.mapreduce.InputSplit], tac)
  }
  
  /**
   * similar to InputSplit.next
   */
  override def fillRecord(split: InputSplit, key: Instance[K], value: Instance[V]): Boolean = {
    val storesplit = getStorehausSplit(split)
    if(storesplit.recordReader.nextKeyValue) {
      val row = storesplit.recordReader.getCurrentValue
      val (cassKey, cassValue) = storeinit.getCascadingRowMatcher.getKeyValueFromRow(row)
      key.set(cassKey)
      value.set(cassValue)
      true
    } else false
  }
  
  private def getStorehausSplit(split: InputSplit): CassandraStorehausSplit = split.asInstanceOf[CassandraStorehausSplit]
  
  /**
   * Translated from org.apache.cassandra.hadoop.ColumnFamilyRecordReader 
   */
  private def getCassandraLocation(split: CassandraStorehausSplit): String = 
    FBUtilities.getAllLocalAddresses.toList.map{address => 
      split.getLocations.toList.find(location => { 
        val result =Try(if (address.equals(InetAddress.getByName(location))) Some(location) else None)
        result.isReturn && result.get.isDefined
      })
    }.find(_.isDefined).flatten.getOrElse(split.getLocations.lift(0).get)
  
  
  /**
   * free resources after splitting is done
   */
  override def close = {}
}