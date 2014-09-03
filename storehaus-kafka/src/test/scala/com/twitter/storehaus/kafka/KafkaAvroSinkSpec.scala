/*
 * Copyright 2013 Twitter inc.
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

import java.util.Date

import kafka.DataTuple
import kafka.consumer.{ConsumerTimeoutException, Whitelist}
import kafka.serializer.Decoder

import org.scalatest.{Matchers, WordSpec}

import com.twitter.util.{Await, Future}

/**
 * Integration Test! Remove .pendingUntilFixed if testing against a Kafka Cluster
 */
class KafkaAvroSinkSpec extends WordSpec with Matchers {
  import KafkaInjections._

  "KafkaAvroSink" should {
    "put avro object on a topic" in pendingUntilFixed {
      val context = new KafkaContext
      import context._

      val topic = "avro-topic-" + random
      val sink = KafkaAvroSink[DataTuple](Seq(zK), topic)
        .filter {
        case (k, v) => v.getValue % 2 == 0
      }

      val futures = (1 to 10)
        .map(new DataTuple(_, "key", new Date().getTime))
        .map(sink.write()("key", _))

      Await.result(Future.collect(futures))

      try {
        val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, implicitly[Decoder[DataTuple]])(0)
        val iterator = stream.iterator()
        iterator.next().message.getValue % 2 shouldBe 0
        iterator.next().message.getValue % 2 shouldBe 0
        iterator.next().message.getValue % 2 shouldBe 0
        iterator.next().message.getValue % 2 shouldBe 0
        iterator.hasNext() shouldBe false
      } catch {
        case e: ConsumerTimeoutException => fail("test failed as consumer timed out without getting any msges")
      }
    }
  }
}
