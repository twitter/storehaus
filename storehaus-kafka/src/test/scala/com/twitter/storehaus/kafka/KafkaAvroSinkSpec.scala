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

import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, BeforeAndAfterAll, WordSpec}
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaAvroSinkSpec extends WordSpec with Matchers with BeforeAndAfterAll with Eventually {

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

  "KafkaAvroSink" should {
    "put avro objects in a topic" in {
      val topic = "avro-topic-" + ktu.random

      implicit val dataTupleInj = SpecificAvroCodecs[DataTuple]
      val sink = KafkaAvroSink[DataTuple](topic, Seq(ktu.brokerAddress))

      val futures = (1 to 10)
        .map(i => ("key", new DataTuple(i.toLong, "key", 1L)))
        .map(sink.write()(_))

      Await.result(Future.collect(futures))
      eventually(timeout(10 seconds), interval(1 second)) {
        val records = {
          import KafkaInjections._
          val consumer = new KafkaConsumer[String, DataTuple](
            ktu.consumerProps, implicitly[Deserializer[String]], implicitly[Deserializer[DataTuple]])
          consumer.subscribe(Seq(topic).asJava)
          consumer.poll(1000).asScala.toList
        }
        records should have size 10
        records.zip(1L to 10L).foreach { case (record, expectedValue) =>
          record.key() shouldBe "key"
          record.value().getValue shouldBe expectedValue
        }
      }
    }
  }
}
