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

import com.twitter.finagle.redis.Client

trait RedisClient {
  def client: Client
  def host: String = "localhost"
  def port: Int = 6379
}

trait DefaultRedisClient extends RedisClient {
  def client: Client = {
    val cli = Client("%s:%d" format(host, port))
    cli.flushDB // clean slate
    cli
  }
}
