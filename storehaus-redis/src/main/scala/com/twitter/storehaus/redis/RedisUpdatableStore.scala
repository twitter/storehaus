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

package com.twitter.storehaus.redis

import com.twitter.finagle.redis.TransactionalClient
import com.twitter.util.{Future, Time}
import com.twitter.storehaus.UpdatableStore
import com.twitter.finagle.redis.protocol.{Set => SetCommand, Del}
import org.jboss.netty.buffer.ChannelBuffer


class RedisUpdatableStore(val txnClient: TransactionalClient, ttl: Option[Time])
  extends RedisStore(txnClient, ttl) with UpdatableStore[ChannelBuffer, ChannelBuffer] {

    def update(k : ChannelBuffer)(fn : Option[ChannelBuffer] => Option[ChannelBuffer]) =
       txnClient.watch(List(k)).flatMap { unit =>
         txnClient.get(k).flatMap { oldValue =>
           val command = fn(oldValue) match {
             case Some(v) => SetCommand(k, v)
             case None => Del(List(k))
           }

           txnClient.transaction(List(command)).flatMap { replies => Future.Unit }
         }
       }
}