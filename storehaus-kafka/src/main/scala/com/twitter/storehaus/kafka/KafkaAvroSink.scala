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

/**
 * @author Mansur Ashraf
 * @since 12/12/13
 */
/**
 * KafkaSink capable of sending Avro messages to a Kafka Topic
 */
@deprecated("use com.twitter.storehaus.kafka.KafkaStore with com.twitter.summingbird.storm.WritableStoreSink")
object KafkaAvroSink {

  import com.twitter.bijection.StringCodec.utf8

  /**
   * Creates KafkaSink that can sends message of form (String,SpecificRecord) to a Kafka Topic
   * @param zkQuorum zookeeper quorum
   * @param topic  Kafka Topic
   * @tparam V  Avro Record
   * @return KafkaSink[String,SpecificRecordBase]
   */
  def apply[V <: SpecificRecordBase : Manifest](zkQuorum: Seq[String], topic: String) = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink(zkQuorum: Seq[String], topic: String)
      .convert[String, V](utf8.toFunction)
    sink
  }

  /**
   * Creates KafkaSink that can sends message of form (T,SpecificRecord) to a Kafka Topic
   * @param zkQuorum zookeeper quorum
   * @param topic  Kafka Topic
   * @tparam V  Avro Record
   * @tparam K key
   * @return KafkaSink[T,SpecificRecordBase]
   */
  def apply[K: Codec, V <: SpecificRecordBase : Manifest](zkQuorum: Seq[String], topic: String) = {
    implicit val inj = SpecificAvroCodecs[V]
    lazy val sink = KafkaSink(zkQuorum: Seq[String], topic: String)
      .convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }
}
