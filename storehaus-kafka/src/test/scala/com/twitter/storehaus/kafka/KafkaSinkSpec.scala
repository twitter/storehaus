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

import org.specs2.mutable.Specification
import com.twitter.bijection.StringCodec.utf8
import com.twitter.util.{Future, Await}
import kafka.consumer.{ConsumerTimeoutException, Whitelist}
import kafka.serializer.Decoder
import KafkaInjections._

/**
 * Integration Test! Remove .pendingUntilFixed if testing against a Kafka Cluster
 * @author Mansur Ashraf
 * @since 12/7/13
 */
class KafkaSinkSpec extends Specification {

  "Kafka Sink" should {

    "be able to convert and sink value" in new KafkaContext {
      val topic = "long_topic-" + random
      val longSink = sink(topic).convert[String, Long](utf8.toFunction)
      Await.result(longSink.write()("1", 1L))
      try {
        val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1,implicitly[Decoder[Long]])(0)
        stream.iterator().next().message === 1L
      } catch {
        case e: ConsumerTimeoutException => failure("test failed as consumer timed out without getting any msges")
      }
    }  .pendingUntilFixed

    "be able to filter value" in new KafkaContext {
      val topic = "filter_topic-" + random
      val filteredSink = sink(topic)
        .convert[String, Long](utf8.toFunction)
        .filter(_._2 % 2 == 0) //only send even values

      val futures = Future.collect((1 to 10).map(v => filteredSink()("key", v)))
      Await.result(futures)
      try {
        val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, implicitly[Decoder[Long]])(0)
        val iterator = stream.iterator()
        iterator.next().message % 2 === 0
        iterator.next().message % 2 === 0
        iterator.next().message % 2 === 0
        iterator.next().message % 2 === 0
        !iterator.hasNext()
      } catch {
        case e: ConsumerTimeoutException => failure("test failed as consumer timed out without getting any msges")
      }
    } .pendingUntilFixed
  }
}
