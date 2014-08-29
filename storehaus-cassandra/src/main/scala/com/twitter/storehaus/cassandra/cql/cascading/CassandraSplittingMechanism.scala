package com.twitter.storehaus.cassandra.cql.cascading

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.storehaus.cascading.InitializableStoreObjectSerializer
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.TFramedTransportFactory
import org.apache.cassandra.hadoop.{ AbstractColumnFamilyInputFormat, ColumnFamilySplit, ConfigHelper }
import com.twitter.util.Try
import com.twitter.storehaus.cascading.Instance
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import org.apache.cassandra.hadoop.cql3.{ CqlRecordReader, CqlInputFormat, CqlConfigHelper }
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import org.apache.cassandra.utils.FBUtilities
import scala.collection.JavaConversions._
import java.net.InetAddress
import org.slf4j.{ Logger, LoggerFactory }

/**
 * Cassandra does not directly support Splitting, so we provide 
 * a special splitting mechanism which is able to split a Cassandra
 */
class CassandraSplittingMechanism[K, V](override val conf: JobConf) 
	extends StorehausSplittingMechanism[K, V](conf: JobConf) {
  @transient private val log = LoggerFactory.getLogger(classOf[CassandraSplittingMechanism[K, V]])
  
  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val storeinit = InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
  
  override def getSplits(job: JobConf, hint: Int) : Array[InputSplit] = {
    // ask for contact information -> call get_splits_ex via ColumnFamilyInputFormat
    log.debug(s"Getting splits for StorehausTap with id $tapid from Cassandra") 
    val connectionOptions = storeinit.getThriftConnections.trim.split(":").map(s => s.trim)
    ConfigHelper.setInputInitialAddress(conf, connectionOptions.head)
    ConfigHelper.setInputRpcPort(conf, if (connectionOptions.size == 1) "9160" else connectionOptions.last)
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName)
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    CqlConfigHelper.setInputColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CqlConfigHelper.setInputNativePort(conf, storeinit.getNativePort.toString)
    val columnFamilyFormat = new CqlInputFormat
    columnFamilyFormat.getSplits(conf, hint).
      map(split => CassandraStorehausSplit(tapid, split.asInstanceOf[ColumnFamilySplit]))
  }
  
  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    val storesplit = getStorehausSplit(split)
    // do this again on the mapper, so we can have multiple taps
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName)
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    CqlConfigHelper.setInputColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CqlConfigHelper.setInputNativePort(conf, storeinit.getNativePort.toString)
    // for some reason the java driver loses this information, so we re-set the default as a hack
    CqlConfigHelper.setInputMinSimultReqPerConnections(conf, "5")
    CqlConfigHelper.setInputMaxSimultReqPerConnections(conf, "128")
    storesplit.recordReader = new CqlRecordReader
    val tac = new TaskAttemptContext(conf, TaskAttemptID.forName(conf.get(AbstractColumnFamilyInputFormat.MAPRED_TASK_ID))) {
      override def progress() = reporter.progress()        
    }
    log.debug(s"Initializing RecordReader for StorehausTap with id $tapid") 
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
      log.debug(s"Filling record for StorehausTap with id $tapid with value=$cassValue and key=$cassKey into store " + storeinit) 
      key.set(cassKey)
      value.set(cassValue)
      true
    } else false
  }
  
  private def getStorehausSplit(split: InputSplit): CassandraStorehausSplit = split.asInstanceOf[CassandraStorehausSplit]
  
  /**
   * free resources after splitting is done
   */
  override def close = {}
}