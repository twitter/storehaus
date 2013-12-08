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

import org.specs2.specification.Scope
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

/**
 * @author Mansur Ashraf
 * @since 12/7/13
 */
trait KafkaContext extends Scope {

  val zK = "localhost:2181"
  lazy val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("KafkaTestPool"))
  val waiter = 10
  val permits = 0

  def store(topic: String) = KafkaStore[String, String](Seq(zK), topic, classOf[StringEncoder])(executor, waiter, permits)

  def sink(topic: String) = KafkaSink(Seq(zK), topic)

  def random = new Random().nextInt(100000)

  //Consumer props
  val props = new Properties()
  props.put("groupid", "consumer-" + random)
  props.put("socket.buffersize", (2 * 1024 * 1024).toString)
  props.put("fetch.size", (1024 * 1024).toString)
  props.put("auto.commit", "true")
  props.put("autocommit.interval.ms", (10 * 1000).toString)
  props.put("autooffset.reset", "smallest")
  props.put("zk.connect", zK)
  props.put("consumer.timeout.ms", (2 * 60 * 1000).toString)
  val config = new ConsumerConfig(props)
  lazy val consumer = Consumer.create(config)
}

class LongDecoder extends Decoder[Long] {
  def toEvent(message: Message): Long = {
    val bytes = Injection[ByteBuffer, Array[Byte]](message.payload)
    Injection.invert[Long, Array[Byte]](bytes).get
  }
}

class AvroDecoder[T<:SpecificRecordBase:Manifest] extends Decoder[T]{
  def toEvent(message: Message): T = {
    val bytes = Injection[ByteBuffer, Array[Byte]](message.payload)
    SpecificAvroCodecs[T].invert(bytes).get
  }
}

