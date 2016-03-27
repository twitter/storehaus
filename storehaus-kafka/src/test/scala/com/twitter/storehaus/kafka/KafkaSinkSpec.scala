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

import com.twitter.util.{Await, Future}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, Deserializer, StringSerializer}
import org.scalatest.{Matchers, WordSpec, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaSinkSpec extends WordSpec with Matchers with BeforeAndAfterAll with Eventually {

  private var ktu: KafkaTestUtils = _

  override protected def beforeAll(): Unit = {
    ktu = new KafkaTestUtils
    ktu.setup()
  }

  override protected def afterAll(): Unit = {
    if (ktu != null) {
      ktu.tearDown()
      ktu = null
    }
  }

  "KafkaSink" should {
    "write messages to a kafka topic" in {
      val topic = "topic-" + ktu.random
      val sink = KafkaSink[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))
      
      val futures = (1 to 10)
        .map(i => ("key", i.toString))
        .map(sink.write()(_))

      Await.result(Future.collect(futures))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(topic)
        records.size shouldBe 10
        records.zip(1 to 10).foreach { case (record, expectedValue) =>
          record.key() shouldBe "key"
          record.value() shouldBe expectedValue.toString
        }
      }
    }
    "write messages to a kafka topic after having been converted" in {
      val topic = "topic-" + ktu.random
      import com.twitter.bijection.StringCodec.utf8
      val sink = KafkaSink[Array[Byte], Array[Byte], ByteArraySerializer, ByteArraySerializer](
          topic, Seq(ktu.brokerAddress))
        .convert[String, String](utf8.toFunction)

      val futures = (1 to 10)
        .map(i => ("key", i.toString))
        .map(sink.write()(_))

      Await.result(Future.collect(futures))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(topic)
        records should have size 10
        records.zip(1 to 10).foreach { case (record, expectedValue) =>
          record.key() shouldBe "key"
          record.value() shouldBe expectedValue.toString
        }
      }
    }
    "write messages to a kafka topic after having been filtered" in {
      val topic = "topic-" + ktu.random
      val sink = KafkaSink[String, String, StringSerializer, StringSerializer](
          topic, Seq(ktu.brokerAddress))
        .filter { case (k , v) => v.toInt % 2 == 0 }

      val futures = (1 to 10)
        .map(i => ("key", i.toString))
        .map(sink.write()(_))

      Await.result(Future.collect(futures))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(topic)
        records.size shouldBe 5
        records.zip((1 to 10).filter(i => i % 2 == 0)).foreach { case (record, expectedValue) =>
          record.key() shouldBe "key"
          record.value() shouldBe expectedValue.toString
        }
      }
    }
  }

  private def getMessages(topic: String): Seq[ConsumerRecord[String, String]] = {
    val consumer = new KafkaConsumer[String, String](ktu.consumerProps)
    consumer.subscribe(Seq(topic).asJava)
    consumer.poll(1000).asScala.toList
  }
}
