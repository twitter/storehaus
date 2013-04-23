package com.twitter.storehaus.redis

import com.twitter.finagle.redis.Client

trait RedisClient {
  def client: Client
}

trait DefaultRedisClient extends RedisClient {
  def client = {
    val client = Client("localhost:6379")
    client.flushDB() // clean slate
    client
  }
}
