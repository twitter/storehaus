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
import java.util.{Properties, Random}

import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.server.{KafkaServer, KafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.collection.JavaConverters._

/**
  * Heavily inspired by Apache Spark's KafkaTestUtils
  *
  * @author BenFradet
  * @since 08/03/16
  */
class KafkaTestUtils {
  private val zkHost = "localhost"
  private var zkPort: Int = 0
  private var zookeeper: EmbeddedZookeeper = _

  private val brokerHost = "localhost"
  private val brokerPort = 9092
  private var brokerConf: KafkaConfig = _
  private var broker: KafkaServer = _

  private var zkReady = false
  private var brokerReady = false

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Broker not setup yet or already torn down, cannot get zookeeper address")
    s"$brokerHost:$brokerPort"
  }

  private def setupEmbeddedZookeeper(): Unit = {
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    zkPort = zookeeper.actualPort
    zkReady = true
  }

  private def setupEmbeddedKafkaBroker(): Unit = {
    assert(zkReady, "Zookeeper should be setup beforehand")
    brokerConf = new KafkaConfig(brokerConfiguration)
    broker = {
      val b = new KafkaServer(brokerConf)
      b.startup()
      b
    }
    brokerReady = true
  }

  def setup(): Unit = {
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaBroker()
  }

  def tearDown(): Unit = {
    brokerReady = false
    zkReady = false

    if (broker != null) {
      broker.shutdown()
      broker = null
    }

    brokerConf.logDirs.foreach(f => deleteFile(new File(f)))

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  def getMessages[K, V](topic: String): Seq[ConsumerRecord[K, V]] = {
    val consumer = new KafkaConsumer[K, V](consumerProps)
    consumer.subscribe(Seq(topic).asJava)
    consumer.poll(10000).asScala.toSeq
  }

  def random = new Random().nextInt(100000)

  def consumerProps = {
    val p = new Properties()
    p.put("bootstrap.servers", brokerAddress)
    p.put("enable.auto.commit", "true")
    p.put("auto.commit.interval.ms", "1000")
    p.put("session.timeout.ms", "30000")
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    p.put("group.id", "consumer-" + random)
    p.put("auto.offset.reset", "earliest")
    p
  }

  private def brokerConfiguration: Properties = {
    val p = new Properties()
    p.put("broker.id", "0")
    p.put("host.name", brokerHost)
    p.put("port", brokerPort.toString)
    p.put("log.dir", Files.createTempDirectory("kafkaTestLogDir").toAbsolutePath.toString)
    p.put("zookeeper.connect", zkAddress)
    p.put("log.flush.interval.messages", "1")
    p.put("replica.socket.timeout.ms", "1500")
    p
  }

  private def deleteFile(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteFile)
    }
    file.delete()
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

    val actualPort = factory.getLocalPort

    def shutdown(): Unit = {
      factory.shutdown()
      deleteFile(snapshotDir)
      deleteFile(logDir)
    }
  }
}

