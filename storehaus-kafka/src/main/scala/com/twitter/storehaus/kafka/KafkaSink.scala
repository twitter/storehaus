package com.twitter.storehaus.kafka

import com.twitter.util.Future

/**
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaSink[K, V](store: KafkaStore[K, V]) {
  def write: () => (((K, V)) => Future[Unit]) = () => {
    u: ((K, V)) => store.put(u)
  }
}

object KafkaSink {
  def apply[K, V](store: KafkaStore[K, V]) = {
    val sink = new KafkaSink[K, V](store)
    sink.write
  }

  def apply[K, V](zkQuorum: Seq[String],
                  topic: String,
                  serializer: Class[_]) = {
    lazy val store = KafkaStore[K, V](zkQuorum, topic, serializer)
    lazy val sink = new KafkaSink[K, V](store)
    sink.write
  }
}