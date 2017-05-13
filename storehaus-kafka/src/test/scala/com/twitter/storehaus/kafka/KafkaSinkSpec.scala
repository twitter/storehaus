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

package com.twitter.storehaus.kafka

import com.twitter.util.{Await, Future}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest.{Matchers, WordSpec, BeforeAndAfterAll}

import scala.collection.JavaConverters._

class KafkaSinkSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private var ktu: KafkaTestUtils = _
  private var consumer: KafkaConsumer[String, String] = _
  private val pollTimeoutMs = 1000
  private val pollTries = 10


  override protected def beforeAll(): Unit = {
    ktu = new KafkaTestUtils
    ktu.setup()
    consumer = new KafkaConsumer[String, String](ktu.consumerProps)
  }

  override protected def afterAll(): Unit = {
    if (consumer != null) {
      consumer.close()
      consumer = null
    }
    if (ktu != null) {
      ktu.tearDown()
      ktu = null
    }
  }

  private def tryReadAtLeastNRecords(n: Int): Array[ConsumerRecord[String, String]] = {
    var allRecords = Array.empty[ConsumerRecord[String, String]]
    for (i <- 1 to pollTries) {
      val records = consumer.poll(pollTimeoutMs).asScala
      allRecords = allRecords ++ records.toList
      if (allRecords.size >= n) {
        return allRecords
      }
    }
    allRecords
  }

  "KafkaSink" should {
    "write messages to a kafka topic" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val sink = KafkaSink[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      val futures = (1 to 10).map(i => sink.write()(("key", i.toString)))

      Await.result(Future.collect(futures))
      val records = tryReadAtLeastNRecords(10)
      records.size shouldBe 10
      records.zip(1 to 10).foreach { case (record, expectedValue) =>
        record.key() shouldBe "key"
        record.value() shouldBe expectedValue.toString
      }
    }
    "write messages to a kafka topic after having been converted" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      import com.twitter.bijection.StringCodec.utf8
      val sink = KafkaSink[Array[Byte], Array[Byte], ByteArraySerializer, ByteArraySerializer](
          topic, Seq(ktu.brokerAddress))
        .convert[String, String](utf8.toFunction)

      val futures = (1 to 10).map(i => sink.write()(("key", i.toString)))

      Await.result(Future.collect(futures))
      val records = tryReadAtLeastNRecords(10)
      records should have size 10
      records.zip(1 to 10).foreach { case (record, expectedValue) =>
        record.key() shouldBe "key"
        record.value() shouldBe expectedValue.toString
      }
    }
    "write messages to a kafka topic after having been filtered" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val sink = KafkaSink[String, String, StringSerializer, StringSerializer](
          topic, Seq(ktu.brokerAddress))
        .filter { case (k, v) => v.toInt % 2 == 0 }

      val futures = (1 to 10).map(i => sink.write()(("key", i.toString)))

      Await.result(Future.collect(futures))
      val records = tryReadAtLeastNRecords(5)
      records.size shouldBe 5
      records.zip((1 to 10).filter(i => i % 2 == 0)).foreach { case (record, expectedValue) =>
        record.key() shouldBe "key"
        record.value() shouldBe expectedValue.toString
      }
    }
  }
}
