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

import com.twitter.storehaus.WritableStore
import com.twitter.util.{Time, FuturePool, Future}
import java.util.Properties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import java.util.concurrent.ExecutorService
import com.twitter.concurrent.AsyncMutex
import kafka.serializer.Encoder


/**
 * Store capable of writing to a Kafka Topic.
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaStore[K, V](topic: String, props: Properties)(executor: => ExecutorService) extends WritableStore[K, V] with Serializable {
  private lazy val producerConfig = new ProducerConfig(props)
  private lazy val producer = new Producer[K, V](producerConfig)
  private lazy val futurePool = FuturePool(executor)
  private[this] lazy val mutex = new AsyncMutex

  /**
   * Puts a key/value pair on a Kafka Topic using kafka.producer.AyncProducer and does not block thread
   * @param kv (key,value)
   * @return Future.unit
   */
  override def put(kv: (K, V)): Future[Unit] = mutex.acquire().flatMap {
    p =>
      futurePool {
        val (key, value) = kv
        producer.send(new KeyedMessage[K, V](topic, key, value))
      } ensure {
        p.release()
      }
  }

  override def multiPut[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = {
    val future = mutex.acquire().flatMap {
      p => futurePool {
        val batch = kvs.map {
          case (k, v) => new KeyedMessage[K, V](topic, k, v)
        }.toList
        producer.send(batch: _*)

      } ensure {
        p.release()
      }
    }
    kvs.mapValues(v => future)
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(time: Time): Future[Unit] = futurePool {
    producer.close()
  }
}

object KafkaStore {

  /**
   * Creates an instance of Kafka store based on given properties.
   * @param topic Kafka topic.
   * @param props Kafka properties { @see http://kafka.apache.org/07/configuration.html}.
   * @tparam K Key
   * @tparam V Value
   * @return Kafka Store
   */
  def apply[K, V](topic: String, props: Properties)(executor: => ExecutorService) = new KafkaStore[K, V](topic, props)(executor)

  /**
   * Creates a Kafka store.
   * @param zkQuorum zookeeper quorum.
   * @param topic  Kafka topic.
   * @tparam K  Key
   * @tparam V Value
   * @return Kafka Store
   */
  def apply[K, V, E <: Encoder[V] : Manifest](zkQuorum: Seq[String],
                                              topic: String)(executor: => ExecutorService) = new KafkaStore[K, V](topic, createProp[V, E](zkQuorum))(executor)


  private def createProp[V, E <: Encoder[V] : Manifest](zkQuorum: Seq[String]): Properties = {
    val prop = new Properties()
    prop.put("serializer.class", implicitly[Manifest[E]].erasure.getName)
    prop.put("zk.connect", zkQuorum.mkString(","))
    prop
  }
}