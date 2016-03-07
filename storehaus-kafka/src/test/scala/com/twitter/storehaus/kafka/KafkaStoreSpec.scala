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

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.WordSpec
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._

/**
  * Integration Test! Replace ignore by should if testing against a running Kafka broker
  * @author Mansur Ashraf
  * @since 12/7/13
  */
class KafkaStoreSpec extends WordSpec {

  "KafkaStore" ignore {
    "put a value on a topic" in {
      val context = KafkaTestUtils()
      val topic = "test-topic-" + context.random

      Await.result(context.store(topic).put(("testKey", "testValue")))
      val consumer = new KafkaConsumer[String, String](context.consumerProps)
      consumer.subscribe(Seq(topic).asJava)
      val records = consumer.poll(10000).asScala
      records.size === 1
      records.head.value() === "testValue"
    }

    "put multiple values on a topic" in {
      val context = KafkaTestUtils()
      val multiputTopic = "multiput-test-topic-" + context.random

      val map = Map(
        "Key_1" -> "value_2",
        "Key_2" -> "value_4",
        "Key_3" -> "value_6"
      )

      val multiputResponse = context.store(multiputTopic).multiPut(map)
      Await.result(Future.collect(multiputResponse.values.toList))
      val consumer = new KafkaConsumer[String, String](context.consumerProps)
      consumer.subscribe(Seq(multiputTopic).asJava)
      val records = consumer.poll(10000).asScala
      records.size === 3
      records.map(_.value()).toSeq === map.values.toSeq
    }
  }
}
