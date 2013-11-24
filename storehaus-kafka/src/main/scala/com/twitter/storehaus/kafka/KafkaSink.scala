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

import com.twitter.util.Future
import KafkaSink.Dispatcher
import kafka.serializer.Encoder
import KafkaEncoders._

/**
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaSink[K, V](dispatcher: Dispatcher[K, V]) {
  def write: () => Dispatcher[K, V] = () => dispatcher

  def convert[K1, V1](fn: ((K1, V1)) => ((K, V))): KafkaSink[K1, V1] = {
    val f: Dispatcher[K1, V1] = {
      x: (K1, V1) => dispatcher(fn(x))
    }
    new KafkaSink[K1, V1](f)
  }

  def filter(fn: ((K, V)) => Boolean): KafkaSink[K, V] = {
    val f: Dispatcher[K, V] = {
      x: (K, V) =>
        if (fn(x)) dispatcher(x)
        else Future.Unit
    }
    new KafkaSink[K, V](f)
  }
}

object KafkaSink {
  type Dispatcher[A, B] = ((A, B)) => Future[Unit]

  def apply[K, V](store: KafkaStore[K, V]): () => Dispatcher[K, V] = {
    val sink = new KafkaSink[K, V](store.put)
    sink.write
  }

  def apply[K, V](zkQuorum: Seq[String],
                  topic: String,
                  serializer: Class[_]): () => Dispatcher[K, V] = {
    lazy val store = KafkaStore[K, V](zkQuorum, topic, serializer)
    lazy val sink = new KafkaSink[K, V](store.put)
    sink.write
  }

  def getSink[K, V](zkQuorum: Seq[String],
                    topic: String,
                    serializer: Class[_]): KafkaSink[K, V] = {
    lazy val store = KafkaStore[K, V](zkQuorum, topic, serializer)
    new KafkaSink[K, V](store.put)
  }
}

object KafkaByteArraySink {
  def apply(zkQuorum: Seq[String], topic: String): () => Dispatcher[Array[Byte], Array[Byte]] = KafkaSink[Array[Byte], Array[Byte]](zkQuorum, topic, classOf[ByteArrayEncoder])

  def apply[T <: Class[Encoder[Array[Byte]]]](zkQuorum: Seq[String], topic: String)(implicit serializer: T = classOf[ByteArrayEncoder]): KafkaSink[Array[Byte], Array[Byte]] = KafkaSink.getSink[Array[Byte], Array[Byte]](zkQuorum, topic, serializer)
}

object KafkaLongSink {
  def apply(zkQuorum: Seq[String], topic: String): () => Dispatcher[String, Long] = KafkaSink[String, Long](zkQuorum, topic, classOf[LongEncoder])

  def apply[T <: Class[Encoder[Long]]](zkQuorum: Seq[String], topic: String)(implicit serializer: T = classOf[LongEncoder]): KafkaSink[String, Long] = KafkaSink.getSink[String, Long](zkQuorum, topic, serializer)
}

