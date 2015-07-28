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

import org.scalatest.WordSpec
import kafka.serializer.Decoder
import com.twitter.util.{Future, Await}
import kafka.consumer.{ConsumerTimeoutException, Whitelist}
import KafkaInjections._

/**
 * Integration Test! Remove .pendingUntilFixed if testing against a Kafka Cluster
 * @author Mansur Ashraf
 * @since 12/7/13
 */
class KafkaStoreSpec extends WordSpec {

  "Kafka store" should {
    "put a value on a topic" in {
      val context = KafkaContext()
      val topic = "test-topic-" + context.random

      Await.result(context.store(topic).put("testKey", "testValue"))
      try {
        val stream = context.consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, implicitly[Decoder[String]], implicitly[Decoder[String]])(0)
        val message = stream.iterator().next().message
        message === "testValue"
      }
      catch {
        case e: ConsumerTimeoutException => fail("test failed as consumer timed out without getting any msges")
      }
    }

    "put multiple values on a topic" in {
      val context = KafkaContext()
      val multiput_topic = "multiput-test-topic-" + context.random

      val map = Map(
        "Key_1" -> "value_2",
        "Key_2" -> "value_4",
        "Key_3" -> "value_6"
      )

      val multiputResponse = context.store(multiput_topic).multiPut(map)
      Await.result(Future.collect(multiputResponse.values.toList))
      try {
        val stream = context.consumer.createMessageStreamsByFilter(new Whitelist(multiput_topic), 1, implicitly[Decoder[String]], implicitly[Decoder[String]])(0)
        val iterator = stream.iterator()
        iterator.next().message.contains("value_")
        iterator.next().message.contains("value_")
        iterator.next().message.contains("value_")
      }
      catch {
        case e: ConsumerTimeoutException => fail("test failed as consumer timed out without getting any msges")
      }
    }
  }
}
