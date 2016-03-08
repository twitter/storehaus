/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.kafka

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.Executors
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.{Properties, Random}
import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.DataTuple
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

/**
  * Heavily inspired by Apache Spark's KafkaTestUtils
  * @author BenFradet
  * @since 08/03/16
  */
class KafkaTestUtils {
  val zk = "localhost:2181"
  val broker = "localhost:9092"
  lazy val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("KafkaTestPool"))
  implicit val dataTupleInj = SpecificAvroCodecs[DataTuple]

  def store(topic: String) = KafkaStore[String, String, StringSerializer, StringSerializer](
    topic, Seq(broker))

  def random = new Random().nextInt(100000)

  val consumerProps = {
    val p = new Properties()
    p.put("bootstrap.servers", broker)
    p.put("enable.auto.commit", "true")
    p.put("auto.commit.interval.ms", "1000")
    p.put("session.timeout.ms", "30000")
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p.put("group.id", "consumer-" + random)
    p.put("auto.offset.reset", "earliest")
    p
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    private val snapshotDir = Files.createTempDirectory("zkTestSnapshotDir").toFile
    private val logDir = Files.createTempDirectory("zkTestLogDir").toFile

    private val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    private val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    private val factory = {
      val f = new NIOServerCnxnFactory
      f.configure(new InetSocketAddress(ip, port), 10)
      f.startup(zookeeper)
      f
    }

    private val actualPort = factory.getLocalPort

    def shutdown(): Unit = {
      factory.shutdown()
      deleteFile(snapshotDir)
      deleteFile(logDir)
    }

    private def deleteFile(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles().foreach(deleteFile)
      }
      file.delete()
    }
  }
}

