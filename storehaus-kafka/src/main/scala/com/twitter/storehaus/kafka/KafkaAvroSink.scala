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

import org.apache.avro.specific.SpecificRecordBase
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.bijection._
import java.util.concurrent.ExecutorService
import java.util.Properties

import org.apache.kafka.common.serialization.ByteArraySerializer

/**
  * KafkaSink capable of sending Avro messages to a Kafka topic
 *
  * @author Mansur Ashraf
  * @since 12/12/13
  */
object KafkaAvroSink {

  import com.twitter.bijection.StringCodec.utf8

  /**
    * Create a KafkaSink that can send messages of the form (String, SpecificRecord) to a Kafka
    * topic
    * @param topic Kafka topic
    * @param brokers Addresses of the Kafka brokers in the hostname:port format
    * @tparam V Type of the Avro record
    * @return KafkaSink[String, SpecificRecordBase]
    */
  def apply[V <: SpecificRecordBase : Manifest](
    topic: String,
    brokers: Seq[String],
    executor: => ExecutorService
  ): KafkaSink[String, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink =
      KafkaSink[Array[Byte], Array[Byte], ByteArraySerializer, ByteArraySerializer](
        topic: String, brokers: Seq[String]).convert[String, V](utf8.toFunction)
    sink
  }

  /**
    * Create KafkaSink that can send messages of the form (T, SpecificRecord) to a Kafka topic
    * @param topic Kafka topic
    * @param brokers Addresses of the Kafka brokers in the hostname:port format
    * @tparam V Type of Avro record
    * @tparam K key
    * @return KafkaSink[T, SpecificRecordBase]
    */
  def apply[K: Codec, V <: SpecificRecordBase : Manifest](
    topic: String,
    brokers: Seq[String]
  ): KafkaSink[K, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink[Array[Byte], Array[Byte], ByteArraySerializer, ByteArraySerializer](
      topic: String, brokers: Seq[String]).convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }

  /**
    * Create a KafkaSink that can send messages of the form (T, SpecificRecord) to a Kafka topic
    * @param topic Kafka topic to produce the messages to
    * @param props Kafka producer properties
    *              { @see http://kafka.apache.org/documentation.html#producerconfigs }
    * @tparam V Type of the Avro record
    * @tparam K key
    * @return KafkaSink[T, SpecificRecordBase]
    */
  def apply[K: Codec, V <: SpecificRecordBase : Manifest](
    topic: String,
    props: Properties
  ): KafkaSink[K, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink[Array[Byte], Array[Byte]](topic, props)
      .convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }
}
