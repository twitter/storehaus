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

import java.util

import com.twitter.bijection.{Codec, Injection}
import org.apache.kafka.common.serialization.{Serializer, Deserializer}

/**
  * @author Mansur Ashraf
  * @since 11/23/13
  */
private[kafka] object KafkaInjections {

  /** Kafka deserializer using twitter-bijection's Injection */
  trait FromInjectionDeserializer[T] extends Deserializer[T] with SerDe {
    def injection: Injection[T, Array[Byte]]

    override def deserialize(topic: String, data: Array[Byte]): T =
      injection.invert(data).get
  }

  /** Kafka serializer using twitter-bijection's Injection */
  trait FromInjectionSerializer[T] extends Serializer[T] with SerDe {
    def injection: Injection[T, Array[Byte]]

    override def serialize(topic: String, data: T): Array[Byte] =
      injection(data)
  }

  /** Trait for the common methods for the Kafka serializer/deserializer */
  trait SerDe {
    def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    def close(): Unit = ()
  }

  def fromInjection[T: Codec]: (Serializer[T], Deserializer[T]) = {
    val result = new FromInjectionSerializer[T] with FromInjectionDeserializer[T] {
      def injection: Injection[T, Array[Byte]] = implicitly[Codec[T]]
    }
    (result, result)
  }

  implicit def injectionSerializer[T: Codec]: Serializer[T] = fromInjection[T]._1

  implicit def injectionDeserializer[T: Codec]: Deserializer[T] = fromInjection[T]._2
}
