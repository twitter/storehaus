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

package com.twitter.storehaus.memcache

import com.twitter.bijection.Injection
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future

import org.jboss.netty.buffer.ChannelBuffer

import scala.util.{ Failure, Success, Try }

class MergeFailedException(val key: String)
  extends RuntimeException("Merge failed for key " + key)

abstract class MergeableMemcacheStore[V](underlying: MemcacheStore)(implicit inj: Injection[V, ChannelBuffer])
  extends ConvertedStore[String, String, ChannelBuffer, V](underlying)(identity)
  with MergeableStore[String, V] {

  protected val MAX_RETRIES = 10 // make this configurable?

  // retryable merge
  protected def doMerge(kv: (String, V), currentRetry: Int) : Future[Option[V]] =
    (currentRetry > MAX_RETRIES) match {
      case false => // use 'gets' api to obtain casunique token
        underlying.client.gets(kv._1).flatMap { res : Option[(ChannelBuffer, ChannelBuffer)] =>
          res match {
            case Some((cbValue, casunique)) =>
              inj.invert(cbValue) match {
                case Success(v) => // attempt cas
                  val resV = semigroup.plus(v, kv._2)
                  underlying.client.cas(kv._1, inj.apply(resV), casunique).flatMap { success =>
                    success.booleanValue match {
                      case true => Future.value(Some(v))
                      case false => doMerge(kv, currentRetry + 1) // retry
                    }
                  }
                case Failure(ex) => Future.exception(ex)
              }
            // no pre-existing value
            case None => put((kv._1, Some(kv._2))).flatMap { u: Unit => Future.None }
          }
        }
      // no more retries
      case true => Future.exception(new MergeFailedException(kv._1))
    }

  override def merge(kv: (String, V)): Future[Option[V]] = doMerge(kv, 1)
}

