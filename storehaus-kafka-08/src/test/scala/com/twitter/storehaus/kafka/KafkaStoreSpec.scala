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

import kafka.consumer.{ConsumerTimeoutException, Whitelist}
import kafka.serializer.Decoder

import org.scalatest.{Matchers, WordSpec}

import com.twitter.util.{Await, Future}

/**
 * Integration Test! Remove .pendingUntilFixed if testing against a Kafka Cluster
 */
class KafkaStoreSpec extends WordSpec with Matchers {
  import KafkaInjections._

  "Kafka store" should {
    "put a value on a topic" in pendingUntilFixed {
      val context = new KafkaContext
      import context._

      val topic = "test-topic-" + random

      Await.result(store(topic).put("testKey", "testValue"))
      try {
        val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, implicitly[Decoder[String]], implicitly[Decoder[String]])(0)
        stream.iterator().next().message shouldBe "testValue"
      } catch {
        case e: ConsumerTimeoutException => fail("test failed as consumer timed out without getting any msges")
      }
    }

    "put multiple values on a topic" in pendingUntilFixed {
      val context = new KafkaContext
      import context._

      val multiput_topic = "multiput-test-topic-" + random

      val map = Map(
        "Key_1" -> "value_2",
        "Key_2" -> "value_4",
        "Key_3" -> "value_6"
      )

      val multiputResponse = store(multiput_topic).multiPut(map)
      Await.result(Future.collect(multiputResponse.values.toList))
      try {
        val stream = consumer.createMessageStreamsByFilter(new Whitelist(multiput_topic), 1, implicitly[Decoder[String]], implicitly[Decoder[String]])(0)
        val iterator = stream.iterator()
        iterator.next().message should contain ("value_")
        iterator.next().message should contain ("value_")
        iterator.next().message should contain ("value_")
      } catch {
        case e: ConsumerTimeoutException => fail("test failed as consumer timed out without getting any msges")
      }
    }
  }
}
