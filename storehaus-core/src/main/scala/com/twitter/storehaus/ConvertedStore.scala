/*
 * Copyright 2013 Twitter Inc.
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

package com.twitter.storehaus

import com.twitter.bijection.Injection
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.util.{ Future, Try }
import scala.util.{ Success, Failure }

/** Use an injection on V2,V1 to convert a store of values V2.
 * If the value stored in the underlying store cannot be converted back to V2, then you will get a Future.exception
 * containing the string "cannot be converted"
 * TODO: we should add a specific exception type here so we can safely filter these cases to Future.None if we so choose.
 */
class ConvertedStore[K1, -K2, V1, V2](store: Store[K1, V1])(kfn: K2 => K1)(implicit inj: Injection[V2, V1])
    extends ConvertedReadableStore[K1, K2, V1, V2](store)(kfn)({ v1: V1 =>
      Future.const(inj.invert(v1).as[Try[V2]])
    })
    with Store[K2, V2] {

  override def put(kv: (K2, Option[V2])) = {
    val k1 = kfn(kv._1)
    val v1 = kv._2.map { inj(_) }
    store.put((k1, v1))
  }
  override def multiPut[K3 <: K2](kvs: Map[K3, Option[V2]]) = {
    val mapK1V1 = kvs.map { case (k3, v2) => (kfn(k3), v2.map(inj(_))) }
    val res: Map[K1, Future[Unit]] = store.multiPut(mapK1V1)
    kvs.keySet.map { k3 => (k3, res(kfn(k3))) }.toMap
  }
  override def close { store.close }
}
