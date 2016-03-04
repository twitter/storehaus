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
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._

/**
 * Integration Test! Remove .pendingUntilFixed if testing against a Kafka Cluster
 * @author Mansur Ashraf
 * @since 12/7/13
 */
class KafkaStoreSpec extends WordSpec {

  "Kafka store" ignore {
    "put a value on a topic" in {
      val context = KafkaContext()
      val topic = "test-topic-" + context.random

      Await.result(context.store(topic).put("testKey", "testValue"))
      context.consumer.subscribe(Seq(topic).asJava)
      val records = context.consumer.poll(100).asScala
      records.head.value() === "testValue"
    }

    "put multiple values on a topic" in {
      val context = KafkaContext()
      val multiputTopic = "multiput-test-topic-" + context.random

      val map = Map(
        "Key_1" -> "value_2",
        "Key_2" -> "value_4",
        "Key_3" -> "value_6"
      )

      val multiputResponse = context.store(multiputTopic).multiPut(map)
      Await.result(Future.collect(multiputResponse.values.toList))
      context.consumer.subscribe(Seq(multiputTopic).asJava)
      val records = context.consumer.poll(100).asScala
      records.size === 3
      records.foreach(record => record.value().contains("value_"))
    }
  }
}
