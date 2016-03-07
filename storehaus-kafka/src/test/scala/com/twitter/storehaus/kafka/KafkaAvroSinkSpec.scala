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
import kafka.DataTuple
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._

/**
  * Integration Test! Replace ignore by should if testing against a running Kafka broker
  * @author Mansur Ashraf
  * @since 12/8/13
  */
class KafkaAvroSinkSpec extends WordSpec {
  "KafkaAvroSink" ignore {
    "put avro object on a topic" in {
      val context = KafkaTestUtils()
      val topic = "avro-topic-" + context.random
      val sink = KafkaAvroSink[DataTuple](topic, Seq(context.broker), context.executor)
        .filter { case (k, v) => v.getValue % 2 == 0 }

      val futures = (1 to 10)
        .map(i => ("key", new DataTuple(i.toLong, "key", 1L)))
        .map(sink.write()(_))

      Await.result(Future.collect(futures))
      val consumer = new KafkaConsumer[String, DataTuple](context.consumerProps)
      consumer.subscribe(Seq(topic).asJava)
      val records = consumer.poll(100).asScala
      records.size === 5
      records.foreach(record => record.value().getValue % 2 === 0)
    }
  }
}
