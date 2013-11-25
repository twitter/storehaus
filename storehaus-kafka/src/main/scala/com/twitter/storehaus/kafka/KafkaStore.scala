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
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaStore[K, V](topic: String, props: Properties) extends WritableStore[K, V] {

  private lazy val producerConfig = new ProducerConfig(props)
  private lazy val producer = new Producer[K, V](producerConfig)

  override def put(kv: (K, V)): Future[Unit] = Future {
    val (key, value) = kv
    producer.send(new ProducerData[K, V](topic, key, List(value)))
  }
}

object KafkaStore {

  def apply[K, V](topic: String, props: Properties) = new KafkaStore[K, V](topic, props)

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