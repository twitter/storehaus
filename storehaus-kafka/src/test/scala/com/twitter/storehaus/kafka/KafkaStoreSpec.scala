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

import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import java.util.concurrent.Executors
import com.twitter.concurrent.NamedPoolThreadFactory
import kafka.serializer.{StringDecoder, StringEncoder}
import com.twitter.util.Await
import java.util.{Random, Properties}
import kafka.consumer.{ConsumerTimeoutException, Whitelist, Consumer, ConsumerConfig}

/**
 * @author Mansur Ashraf
 * @since 12/7/13
 */
class KafkaStoreSpec extends Specification {

  "Kafka store" should {
    "put a value on a topic" in new KafkaContext {
      Await.result(store.put("testKey", "testValue"))
      try {
        val stream = consumer.createMessageStreamsByFilter[String](new Whitelist(topic), 1, new StringDecoder)(0)
        stream.iterator().next().message === "testValue"
      }
      catch {
        case e: ConsumerTimeoutException => failure("test failed as consumer timed out without getting any msges")
      }
    }
  }
}

trait KafkaContext extends Scope {

  val zK = "localhost:2181"
  lazy val executor = Executors.newCachedThreadPool(new NamedPoolThreadFactory("KafkaTestPool"))
  val waiter = 0
  val permits = 0
  val topic = "test-" + new Random().nextInt(100000)
  lazy val store = KafkaStore[String, String](Seq(zK), topic, classOf[StringEncoder])(executor, waiter, permits)

  //Consumer props
  val props = new Properties()
  props.put("groupid", "consumer-" + new Random().nextInt(100000))
  props.put("socket.buffersize", (2 * 1024 * 1024).toString)
  props.put("fetch.size", (1024 * 1024).toString)
  props.put("auto.commit", "true")
  props.put("autocommit.interval.ms", (10 * 1000).toString)
  props.put("autooffset.reset", "smallest")
  props.put("zk.connect", zK)
  props.put("consumer.timeout.ms", (2 * 60 * 1000).toString)
  val config = new ConsumerConfig(props)
  val topicCountMap = Map(topic -> 1)
  lazy val consumer = Consumer.create(config)
}
