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
import com.twitter.bijection.{Codec, Injection}
import org.apache.avro.specific.SpecificRecordBase
import com.twitter.bijection.avro.AvroCodecs

/**
 * @author Mansur Ashraf
 * @since 11/22/13
 */
class KafkaSink[K, V](dispatcher: Dispatcher[K, V]) {
  def write: () => Dispatcher[K, V] = () => dispatcher

  def convert[K1, V1](kfn: K1 => K)(implicit inj: Injection[V1, V]) = {
    val fn: Dispatcher[K1, V1] = {
      kv: (K1, V1) => dispatcher(compose(kfn, inj)(kv))
    }
    new KafkaSink[K1, V1](fn)
  }

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
  type Dispatcher[A, B] = ((A, B)) => Future[Unit]

  def apply[K, V](store: KafkaStore[K, V]) = {
    lazy val sink = new KafkaSink[K, V](store.put)
    sink
  }

  def apply[K, V](zkQuorum: Seq[String],
                  topic: String, serializer: Class[_]) = {
    lazy val store = KafkaStore[K, V](zkQuorum, topic, serializer)
    lazy val sink = apply[K, V](store)
    sink
  }

  def apply(zkQuorum: Seq[String],
            topic: String) = {
    import KafkaEncoders.byteArrayEncoder
    apply[Array[Byte], Array[Byte]](zkQuorum, topic, byteArrayEncoder)
  }
}

object KafkaAvroSink {

  import com.twitter.bijection.StringCodec.utf8

  def apply[V <: SpecificRecordBase : Manifest](zkQuorum: Seq[String], topic: String) = {
    implicit val inj = AvroCodecs[V]
    lazy val sink = KafkaSink(zkQuorum: Seq[String], topic: String)
      .convert[String, V](utf8.toFunction)
    sink
  }

  def apply[K: Codec, V <: SpecificRecordBase : Manifest](zkQuorum: Seq[String], topic: String) = {
    implicit val inj = AvroCodecs[V]
    lazy val sink = KafkaSink(zkQuorum: Seq[String], topic: String)
      .convert[K, V](implicitly[Codec[K]].toFunction)
    sink
  }
}

