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

import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.scalatest.{Matchers, BeforeAndAfterAll, WordSpec}
import com.twitter.util.{Future, Await}

import scala.collection.JavaConverters._

class KafkaAvroSinkSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  private var ktu: KafkaTestUtils = _
  private var consumer: KafkaConsumer[String, DataTuple] = _
  private val pollTimeoutMs = 60000

  override protected def beforeAll(): Unit = {
    ktu = new KafkaTestUtils
    ktu.setup()
    import KafkaInjections._
    implicit val dataTupleInj = SpecificAvroCodecs[DataTuple]
    consumer = new KafkaConsumer[String, DataTuple](
      ktu.consumerProps, implicitly[Deserializer[String]], implicitly[Deserializer[DataTuple]])
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

  "KafkaAvroSink" should {
    "put avro objects in a topic" in {
      val topic = "topic-" + ktu.random
      consumer.subscribe(Seq(topic).asJava)

      implicit val dataTupleInj = SpecificAvroCodecs[DataTuple]
      val sink = KafkaAvroSink[DataTuple](topic, Seq(ktu.brokerAddress))

      val futures = (1 to 10).map(i => sink.write()(("key", new DataTuple(i.toLong, "key", 1L))))

      Await.result(Future.collect(futures))
      val records = consumer.poll(pollTimeoutMs).asScala
      records should have size 10
      records.zip(1L to 10L).foreach { case (record, expectedValue) =>
        record.key() shouldBe "key"
        record.value().getValue shouldBe expectedValue
      }
      consumer.unsubscribe()
    }
  }
}
