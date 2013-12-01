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

import com.twitter.storehaus.WritableStore
import com.twitter.util.Future
import java.util.Properties
import kafka.producer.{ProducerData, Producer, ProducerConfig}


/**
 * Store capable of writing to a Kafka Topic.
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaStore[K, V](topic: String, props: Properties) extends WritableStore[K, V] {
  props.put("producer.type", "async")
  //force async producer
  private lazy val producerConfig = new ProducerConfig(props)
  private lazy val producer = new Producer[K, V](producerConfig)

  /**
   * Puts a key/value pair on a Kafka Topic using kafka.producer.AyncProducer and does not block thread
   * @param kv (key,value)
   * @return Future.unit
   */
  override def put(kv: (K, V)): Future[Unit] = Future {
    val (key, value) = kv
    producer.send(new ProducerData[K, V](topic, key, List(value)))
  }

  /** Replace a set of keys at one time */
  override def multiPut[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = {

    kvs.map{case (k,v) => new ProducerData[K, V](topic, k, List(v))}

    val batch = kvs.foldLeft(List[ProducerData[K, V]]()) {
      case (seed, kv) => new ProducerData[K, V](topic, kv._1, List(kv._2)) :: seed
    }
    val future = Future {
      producer.send(batch: _*)
    }
    kvs.mapValues(v => future.unit)
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
  def apply[K, V](topic: String, props: Properties) = new KafkaStore[K, V](topic, props)

  /**
   * Creates a Kafka store.
   * @param zkQuorum zookeeper quorum.
   * @param topic  Kafka topic.
   * @param serializer message encoder { @see http://kafka.apache.org/07/quickstart.html}
   * @tparam K  Key
   * @tparam V Value
   * @return Kafka Store
   */
  def apply[K, V](zkQuorum: Seq[String],
                  topic: String,
                  serializer: Class[_]) = new KafkaStore[K, V](topic, createProp(zkQuorum, serializer))


  private def createProp(zkQuorum: Seq[String],
                         serializer: Class[_]): Properties = {
    val prop = new Properties()
    prop.put("serializer.class", serializer.getName)
    prop.put("zk.connect", zkQuorum.mkString(","))
    prop
  }
}