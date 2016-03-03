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
import kafka.serializer.DefaultEncoder
import java.util.Properties

/**
 * @author Mansur Ashraf
 * @since 12/12/13
 */
/**
 * KafkaSink capable of sending Avro messages to a Kafka Topic
 */
object KafkaAvroSink {

  import com.twitter.bijection.StringCodec.utf8

  /**
   * Creates KafkaSink that can sends message of form (String,SpecificRecord) to a Kafka Topic
   * @param brokers kafka brokers
   * @param topic  Kafka Topic
   * @tparam V  Avro Record
   * @return KafkaSink[String,SpecificRecordBase]
   */
  def apply[V <: SpecificRecordBase : Manifest](brokers: Seq[String], topic: String, executor: => ExecutorService): KafkaSink[String, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink[Array[Byte], Array[Byte], DefaultEncoder](brokers: Seq[String], topic: String)
      .convert[String, V](utf8.toFunction)
    sink
  }

  /**
   * Creates KafkaSink that can sends message of form (T,SpecificRecord) to a Kafka Topic
   * @param brokers kafka brokers
   * @param topic  Kafka Topic
   * @tparam V  Avro Record
   * @tparam K key
   * @return KafkaSink[T,SpecificRecordBase]
   */
  def apply[K: Codec, V <: SpecificRecordBase : Manifest](brokers: Seq[String], topic: String): KafkaSink[K, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink[Array[Byte], Array[Byte], DefaultEncoder](brokers: Seq[String], topic: String)
      .convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }

  /**
   * Creates KafkaSink that can sends message of form (T,SpecificRecord) to a Kafka Topic
   * @param props kafka props
   * @param topic  Kafka Topic
   * @tparam V  Avro Record
   * @tparam K key
   * @return KafkaSink[T,SpecificRecordBase]
   */
  def apply[K: Codec, V <: SpecificRecordBase : Manifest](props: Properties, topic: String): KafkaSink[K, V] = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink[Array[Byte], Array[Byte]](props, topic: String)
      .convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }
}
