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

import kafka.serializer.{Decoder, Encoder}
import com.twitter.bijection.{Codec, Injection}

/**
 * @author Mansur Ashraf
 * @since 11/23/13
 */
object KafkaInjections {

  class ByteArrayEncoder extends FromInjectionEncoder[Array[Byte]] {
    def injection: Injection[Array[Byte], Array[Byte]] = Injection.identity[Array[Byte]]
  }

  trait FromInjectionDecoder[T] extends Decoder[T] {
    def injection: Injection[T, Array[Byte]]

    override def fromBytes(bytes: Array[Byte]): T = injection.invert(bytes).get
  }

  trait FromInjectionEncoder[T] extends Encoder[T] {
    def injection: Injection[T, Array[Byte]]

    override def toBytes(t: T): Array[Byte] = injection(t)
  }

  def fromInjection[T: Codec]: (Encoder[T], Decoder[T]) = {
    val result = new FromInjectionEncoder[T] with FromInjectionDecoder[T] {
      def injection: Injection[T, Array[Byte]] = implicitly[Codec[T]]
    }
    (result, result)
  }

  implicit def injectionEncoder[T: Codec] = fromInjection[T]._1

  implicit def injectionDecoder[T: Codec] = fromInjection[T]._2

}
