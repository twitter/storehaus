package com.twitter.storehaus.memcache.compat

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.KetamaClientBuilder
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.util.Duration

object MemcacheCompatClient {

  // FIXME use Stack.Params
  def defaultClient(
    name: String,
    nodeString: String,
    retries: Int,
    timeout: Duration,
    hostConnectionLimit: Int): Client = {
    val builder = ClientBuilder()
      .name(name)
      .retries(retries)
      .tcpConnectTimeout(timeout)
      .requestTimeout(timeout)
      .connectTimeout(timeout)
      // FIXME use SessionParam.maxIdleTime once migrated to Stack params
      // .readerIdleTimeout(timeout)
      .hostConnectionLimit(hostConnectionLimit)
      .codec(Memcached())

    KetamaClientBuilder()
      .clientBuilder(builder)
      .nodes(nodeString)
      .build()
  }

}