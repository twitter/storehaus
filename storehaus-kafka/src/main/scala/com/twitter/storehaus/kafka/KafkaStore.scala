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

import java.util.Properties

import com.twitter.storehaus.WritableStore
import com.twitter.util.{Promise, Time, Future}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata, ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.Serializer

import scala.reflect.ClassTag

/**
  * Store capable of writing to a Kafka topic.
  * @author Mansur Ashraf
  * @since 11/22/13
  */
class KafkaStore[K, V](topic: String, props: Properties)
  extends WritableStore[K, V] with Serializable {

  private lazy val producer = new KafkaProducer[K, V](props)

  /**
    * Put a key/value pair in a Kafka topic
    * @param kv (key, value)
    * @return Future.unit
    */
  override def put(kv: (K, V)): Future[Unit] =
    putAndRetrieveMetadata(kv).map(_ => ())

  /**
    * Put a key/value pair in a Kafka topic and retrieve record metadata
    * @param kv (key, value)
    * @return Future[RecordMetadata]
    */
  def putAndRetrieveMetadata(kv: (K, V)): Future[RecordMetadata] = {
    val promise = new Promise[RecordMetadata]()
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) promise.setException(exception)
        else promise.setValue(metadata)
      }
    }
    val (key, value) = kv
    producer.send(new ProducerRecord[K, V](topic, key, value), callback)
    promise
  }

  /**
    * Close this store and release any resources.
    * It is undefined what happens on put/multiGet after close
    */
  override def close(time: Time): Future[Unit] = Future {
    producer.close()
  }
}

object KafkaStore {

  /**
    * Create a KafkaStore based on the given properties
    * @param topic Kafka topic to produce the messages to
    * @param props Kafka producer properties
    *              { @see http://kafka.apache.org/documentation.html#producerconfigs }
    * @return Kafka Store
    */
  def apply[K, V](topic: String, props: Properties): KafkaStore[K, V] =
    new KafkaStore[K, V](topic, props)

  /**
    * Create a KafkaStore
    * @param topic Kafka topic to produce the messages to
    * @param brokers Addresses of the Kafka brokers in the hostname:port format
    * @return Kafka Store
    */
  def apply[K, V, KS <: Serializer[K] : ClassTag, VS <: Serializer[V] : ClassTag](
    topic: String,
    brokers: Seq[String]
  ): KafkaStore[K, V] = new KafkaStore[K, V](topic, createProps[K, V, KS, VS](brokers))


  private def createProps[K, V, KS <: Serializer[K] : ClassTag, VS <: Serializer[V] : ClassTag](
    brokers: Seq[String]
  ): Properties = {
    val props = new Properties()
    props.put("key.serializer", implicitly[ClassTag[KS]].runtimeClass.getName)
    props.put("value.serializer", implicitly[ClassTag[VS]].runtimeClass.getName)
    props.put("bootstrap.servers", brokers.mkString(","))
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props
  }
}
