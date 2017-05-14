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

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{Matchers, BeforeAndAfterAll, WordSpec}
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._

class KafkaStoreSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private var ktu: KafkaTestUtils = _
  private var consumer: KafkaConsumer[String, String] = _
  private val pollTimeoutMs = 60000

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

  "KafkaStore" should {
    "put a value in a topic" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      Await.result(store.put(("testKey", "testValue")))
      val records = consumer.poll(pollTimeoutMs).asScala
      records should have size 1
      records.head.value() shouldBe "testValue"
      consumer.unsubscribe()
    }

    "put a value in a topic and retrieve its metadata" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      val recordMetadata = Await.result(store.putAndRetrieveMetadata(("testKey", "testValue")))
      val records = consumer.poll(pollTimeoutMs).asScala
      records should have size 1
      records.head.value() shouldBe "testValue"
      recordMetadata.topic() shouldBe topic
      recordMetadata.offset() shouldBe 0L
      recordMetadata.partition() shouldBe 0
      consumer.unsubscribe()
    }

    "put multiple values in a topic" in {
      val topic = "test-topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      val map = Map(
        "key1" -> "value2",
        "key2" -> "value4",
        "key3" -> "value6"
      )

      Await.result(Future.collect(store.multiPut(map).values.toList))
      val records = consumer.poll(pollTimeoutMs).asScala
      records should have size 3
      records.map(_.value()) shouldBe map.values.toSeq
      consumer.unsubscribe()
    }

    "set the ex in the future if a problem occurred" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      val store = KafkaStore[String, String, StringSerializer, StringSerializer](
        topic, Seq(ktu.brokerAddress))

      store.close()
      intercept[Exception] {
        Await.result(store.put(("testKey", "testValue")))
      }
      consumer.unsubscribe()
    }
  }
}
