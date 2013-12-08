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
import com.twitter.bijection.StringCodec.utf8
import com.twitter.util.Await
import kafka.consumer.Whitelist
import com.twitter.bijection.Injection

/**
 * @author Muhammad Ashraf
 * @since 12/7/13
 */
class KafkaSinkSpec extends Specification {

  "Kafka Sink" should {

    "be able to convert and sink value" in new KafkaContext {
      val topic = "long_topic-" + random
      val longStore = sink(topic).convert[String, Long](utf8.toFunction)
      Await.result(longStore.write()("1", 1L))
      private val bytes = Injection[Long,Array[Byte]](1)
      Injection.invert[Long,Array[Byte]](bytes)
      val stream = consumer.createMessageStreamsByFilter(new Whitelist(topic), 1, new LongDecoder)(0)
      stream.iterator().next().message === 1L
    }
  }
}
