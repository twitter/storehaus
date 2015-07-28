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

import java.util.concurrent.Executors
import com.twitter.concurrent.NamedPoolThreadFactory
import java.util.{Properties, Random}
import kafka.serializer.{Decoder, StringEncoder}
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.message.Message
import com.twitter.bijection.Injection
import java.nio.ByteBuffer
import org.apache.avro.specific.SpecificRecordBase
import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.DataTuple

/**
 * @author Mansur Ashraf
 * @since 12/7/13
 */
case class KafkaContext() {

  val zK = "localhost:2181"
  val broker = "localhost:9092"
  lazy val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("KafkaTestPool"))
  implicit val dataTupleInj= SpecificAvroCodecs[DataTuple]

  def store(topic: String) = KafkaStore[String, String,StringEncoder](Seq(broker), topic)

  def random = new Random().nextInt(100000)

  //Consumer props
  val props = new Properties()
  props.put("group.id", "consumer-"+random)
  props.put("autocommit.interval.ms", 1000.toString)
  props.put("zookeeper.connect", zK)
  props.put("consumer.timeout.ms", (60 * 1000).toString)
  props.put("auto.offset.reset", "smallest")
  val config = new ConsumerConfig(props)
  lazy val consumer = Consumer.create(config)
}

