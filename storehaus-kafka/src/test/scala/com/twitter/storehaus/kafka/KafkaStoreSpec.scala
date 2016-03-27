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

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, BeforeAndAfterAll, WordSpec}
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaStoreSpec extends WordSpec with Matchers with BeforeAndAfterAll with Eventually {

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

  "KafkaStore" should {
    "put a value in a topic" in {
      val topic = "test-topic-" + ktu.random
      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      Await.result(store.put(("testKey", "testValue")))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(topic)
        records should have size 1
        records.head.value() shouldBe "testValue"
      }
    }
    
    "put a value in a topic and retrieve its metadata" in {
      val topic = "test-topic-" + ktu.random
      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      val recordMetadata = Await.result(store.putAndRetrieveMetadata(("testKey", "testValue")))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(topic)
        records should have size 1
        records.head.value() shouldBe "testValue"
        recordMetadata.topic() shouldBe topic
        recordMetadata.offset() shouldBe 0L
        recordMetadata.partition() shouldBe 0
      }
    }

    "put multiple values in a topic" in {
      val multiputTopic = "multiput-test-topic-" + ktu.random
      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        multiputTopic, Seq(ktu.brokerAddress))

      val map = Map(
        "Key_1" -> "value_2",
        "Key_2" -> "value_4",
        "Key_3" -> "value_6"
      )

      val multiputResponse = store.multiPut(map)
      Await.result(Future.collect(multiputResponse.values.toList))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = getMessages(multiputTopic)
        records should have size 3
        records.map(_.value()) shouldBe map.values.toSeq
      }
    }
  }

  private def getMessages(topic: String): Seq[ConsumerRecord[String, String]] = {
    val consumer = new KafkaConsumer[String, String](ktu.consumerProps)
    consumer.subscribe(Seq(topic).asJava)
    consumer.poll(1000).asScala.toSeq
  }
}
