/*
 * Copyright 2014 Twitter, Inc.
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

import org.apache.hadoop.mapred.{ InputSplit, JobConf, Reporter }
import com.twitter.storehaus.cascading.InitializableStoreObjectSerializer
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.TFramedTransportFactory
import org.apache.cassandra.hadoop.{ AbstractColumnFamilyInputFormat, ColumnFamilySplit, ConfigHelper }
import com.twitter.util.{ Throw, Try }
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
import java.io.IOException

/**
 * Cassandra does not directly support Splitting, so we provide
 * a special splitting mechanism which is able to split a Cassandra
 */
class CassandraSplittingMechanism[K, V, U <: CassandraCascadingInitializer[K, V]](override val conf: JobConf)
  extends StorehausSplittingMechanism[K, V, U](conf: JobConf) {
  @transient private val log = LoggerFactory.getLogger(classOf[CassandraSplittingMechanism[K, V, U]])

  val SPLIT_RETRIES = "com.twitter.storehaus.cassandra.cql.cascading.splitretries"

  val tapid = InitializableStoreObjectSerializer.getTapId(conf)
  val readVersion = InitializableStoreObjectSerializer.getReadVerion(conf, tapid)
  val storeinit = readVersion match {
    case None          => InitializableStoreObjectSerializer.getReadableStoreIntializer(conf, tapid).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
    case Some(version) => InitializableStoreObjectSerializer.getReadableVersionedStoreIntializer(conf, tapid, version).get.asInstanceOf[CassandraCascadingInitializer[K, V]]
  }
  lazy val rowMatcher = storeinit.getCascadingRowMatcher

  override def getSplits(job: JobConf, hint: Int): Array[InputSplit] = {
    val retryMaxNumber = Try(Option(conf.get(SPLIT_RETRIES)).map(_.toInt)).toOption.flatten.getOrElse(10)
    def retrySplittingMechnism(tryNumber: Int, columnFamilyFormat: CqlInputFormat): Try[Array[CassandraStorehausSplit]] = {
      Try(columnFamilyFormat.getSplits(conf, hint).map(split =>
        CassandraStorehausSplit(tapid, split.asInstanceOf[ColumnFamilySplit]))).
        onFailure { e =>
          if (tryNumber < retryMaxNumber) {
            log.warn(s"Retrying getting splits due to error:", e)
            retrySplittingMechnism(tryNumber + 1, columnFamilyFormat)
          } else {
            Throw(e)
          }
        }
    }
    // ask for contact information -> call get_splits_ex via ColumnFamilyInputFormat
    log.debug(s"Getting splits for StorehausTap with id $tapid from Cassandra")
    val connectionOptions = storeinit.getThriftConnections.trim.split(",")(0).split(":").map(s => s.trim)
    ConfigHelper.setInputInitialAddress(conf, connectionOptions.head)
    ConfigHelper.setInputRpcPort(conf, if (connectionOptions.size == 1) "9160" else connectionOptions.last)
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName(readVersion))
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    CqlConfigHelper.setInputColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CqlConfigHelper.setInputNativePort(conf, storeinit.getNativePort.toString)
    val columnFamilyFormat = new CqlInputFormat
    val splits = retrySplittingMechnism(0, columnFamilyFormat).onFailure { e =>
      log.error(s"Got Exception from Cassandra while getting splits on seeds ${ConfigHelper.getInputInitialAddress(conf)}", e)
      throw e
    }
    splits.get.toArray
  }

  override def initializeSplitInCluster(split: InputSplit, reporter: Reporter): Unit = {
    val storesplit = getStorehausSplit(split)
    // if desired, throttle speed of how fast mappers are created
    ThrottlingConfig.waitConfiguredSeconds(conf)
    // do this again on the mapper, so we can have multiple taps
    ConfigHelper.setInputColumnFamily(conf, storeinit.getKeyspaceName, storeinit.getColumnFamilyName(readVersion))
    ConfigHelper.setInputPartitioner(conf, storeinit.getPartitionerName)
    CqlConfigHelper.setInputColumns(conf, storeinit.getCascadingRowMatcher.getColumnNamesString)
    CqlConfigHelper.setInputNativePort(conf, storeinit.getNativePort.toString)
    // for some reason the java driver loses this information, so we re-set the default as a hack
    CqlConfigHelper.setInputMinSimultReqPerConnections(conf, "5")
    CqlConfigHelper.setInputMaxSimultReqPerConnections(conf, "128")
    // CQL-Cassandra-Hadoop is buggy right now, so we double the input-size compared with the default one
    // see https://github.com/AndreasPetter/cassandra-hadoop-vnodes for an alternative
    CqlConfigHelper.setInputCQLPageRowSize(conf, (2 * ConfigHelper.getInputSplitSize(conf)).toString)
    // CqlConfigHelper.setInputCql(conf, storeinit.getUserDefinedWhereClauses(readVersion))
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
    if (storesplit.recordReader.nextKeyValue) {
      val row = storesplit.recordReader.getCurrentValue
      val (cassKey, cassValue) = rowMatcher.getKeyValueFromRow(row)
      log.debug(s"Filling record for StorehausTap with id $tapid with value=$cassValue and key=$cassKey")
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

  /**
   * free record reader
   */
  override def closeSplit(split: InputSplit) = getStorehausSplit(split).recordReader.close()

}

object ThrottlingConfig {
    @transient private val log = LoggerFactory.getLogger("ThrottlingConfig")
  
  val MAPPING_THROTTLING_SECONDS = "com.twitter.storehaus.cassandra.cql.cascading.mapthrottlesecs"

  def waitConfiguredSeconds(conf: JobConf): Unit = Option(conf.get(MAPPING_THROTTLING_SECONDS)) match {
    case Some(seconds) => Try {
      val secs = seconds.toInt
      log.info(s"Mapper is waiting configured $secs seconds to throttle query speed.")
      Thread.sleep(secs * 1000L)
    }.onFailure(_ match {
      case n: NumberFormatException => log.error(s"$seconds does not seem to be an int described by $MAPPING_THROTTLING_SECONDS", n)
      case e: Exception             => log.error(s"Exception on waiting for $seconds as configured for $MAPPING_THROTTLING_SECONDS", e)
    })
    case None =>
  }
}

