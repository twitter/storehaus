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

import com.twitter.util.Future
import KafkaSink.Dispatcher
import com.twitter.bijection.Injection
import scala.Array
import java.util.concurrent.{Executors, ExecutorService}
import com.twitter.concurrent.NamedPoolThreadFactory
import kafka.serializer.Encoder
import com.twitter.storehaus.kafka.KafkaInjections.ByteArrayEncoder
import java.util.Properties

/**
 * Kafka Sink that can be used with SummingBird to sink messages to a Kafka Queue
 * @author Mansur Ashraf
 * @since 11/22/13
 */
@deprecated("use com.twitter.storehaus.kafka.KafkaStore with com.twitter.summingbird.storm.WritableStoreSink","0.9.0")
class KafkaSink[K, V](dispatcher: Dispatcher[K, V]) extends Serializable {
  /**
   * Function that satisfies Storm#Sink
   * @return  () => (K,V) => Future[Unit]
   */
  def write: () => Dispatcher[K, V] = () => dispatcher

  /**
   * Converts KafkaSink[k,V] to KafkaSink[k1,V1]
   * @param kfn function that converts K1 to K
   * @param inj  injection from V1 to V
   * @tparam K1  new Store Key
   * @tparam V1  new Store Value
   * @return   KafkaSink[k1,V1]
   */
  def convert[K1, V1](kfn: K1 => K)(implicit inj: Injection[V1, V]) = {
    val fn: Dispatcher[K1, V1] = {
      kv: (K1, V1) => dispatcher(compose(kfn, inj)(kv))
    }
    new KafkaSink[K1, V1](fn)
  }

  /**
   * Filter all the messages that do not satisfy the given predicate
   * @param fn  predicate
   * @return KafkaSink
   */
  def filter(fn: ((K, V)) => Boolean) = {
    val f: Dispatcher[K, V] = {
      kv: (K, V) =>
        if (fn(kv)) dispatcher(kv)
        else Future.Unit
    }
    new KafkaSink[K, V](f)
  }

  private def compose[K1, V1](kfn: K1 => K, inj: Injection[V1, V]): ((K1, V1)) => ((K, V)) = {
    case (k: K1, v: V1) => (kfn(k), inj(v))
  }
}

object KafkaSink {

  type Dispatcher[K, V] = ((K, V)) => Future[Unit]

  /**
   * Creates KafkaSink by wrapping KafkaStore
   * @param store KafkaStore
   * @tparam K key
   * @tparam V value
   * @return KafkaSink
   */
  def apply[K, V](store: KafkaStore[K, V]): KafkaSink[K, V] = {
    val sink = new KafkaSink[K, V](store.put)
    sink
  }

  /**
   * Returns KafkaSink[K,V]
   * @param brokers  kafka brokers
   * @param topic kafka topic.
   * @tparam K key
   * @tparam V value
   * @return KafkaSink[K,V]
   */
  def apply[K, V, E <: Encoder[V] : Manifest](brokers: Seq[String], topic: String): KafkaSink[K, V] = {
    val store = KafkaStore[K, V, E](brokers, topic)
    lazy val sink = apply[K, V](store)
    sink
  }

  /**
   * Returns KafkaSink[K,V]
   * @param props  kafka props
   * @param topic kafka topic.
   * @tparam K key
   * @tparam V value
   * @return KafkaSink[K,V]
   */
  def apply[K, V](props: Properties, topic: String): KafkaSink[K, V] = {
    lazy val store = KafkaStore[K, V](topic, props)
    lazy val sink = apply[K, V](store)
    sink
  }
}

