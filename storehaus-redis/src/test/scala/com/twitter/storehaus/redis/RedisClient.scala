package com.twitter.storehaus.redis

import com.twitter.finagle.redis.Client

trait RedisClient {
  def client: Client
  def host: String = "localhost"
  def port: Int = 6379
}

trait DefaultRedisClient extends RedisClient {
  def client = {
    val cli = Client("%s:%d" format(host, port))
    cli.flushDB // clean slate
    cli
  }
}
