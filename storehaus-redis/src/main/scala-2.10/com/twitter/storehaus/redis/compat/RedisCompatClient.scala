package com.twitter.storehaus.redis.compat

import com.twitter.finagle.redis
import com.twitter.finagle.redis.protocol.ZRangeResults
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

object RedisCompatClient {

  def get(client: redis.Client, k: ChannelBuffer): Future[Option[ChannelBuffer]] =
    client.get(k)

  def mGet2(client: redis.Client, ks: Seq[ChannelBuffer]): Future[Seq[Option[ChannelBuffer]]] =
    client.mGet2(ks)

  def set(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer): Future[Unit] =
    client.set(k, v)

  def setEx(client: redis.Client, k: ChannelBuffer, t: Long, v: ChannelBuffer): Future[Unit] =
    client.setEx(k, t, v)

  def hGetAll(client: redis.Client, k: ChannelBuffer): Future[Seq[(ChannelBuffer, ChannelBuffer)]] =
    client.hGetAll(k)

  def hMSet(client: redis.Client, k: ChannelBuffer, m: Map[ChannelBuffer, ChannelBuffer]): Future[Unit] =
    client.hMSet(k, m)

  def expire(client: redis.Client, k: ChannelBuffer, t: Int) =
    client.expire(new ChannelBufferBuf(k), t)

  def del(client: redis.Client, ks: Seq[ChannelBuffer]) =
    client.dels(ks map (new ChannelBufferBuf(_)))

  def incrBy(client: redis.Client, k: ChannelBuffer, by: Long) =
    client.incrBy(new ChannelBufferBuf(k), by)

  def sMembers(client: redis.Client, k: ChannelBuffer): Future[Set[ChannelBuffer]] =
    client.sMembers(k)

  def sAdd(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.sAdd(k, vs)

  def sRem(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.sRem(k, vs)

  def sIsMember(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer) =
    client.sIsMember(k, v)

  def zRange(client: redis.Client, k: ChannelBuffer, from: Long, to: Long, t: Boolean): Future[Either[ZRangeResults,Seq[ChannelBuffer]]] =
    client.zRange(k, from, to, t)

  def zRangeLeftAsTuples(client: redis.Client, k: ChannelBuffer, from: Long, to: Long, t: Boolean): Future[Option[Seq[(ChannelBuffer, Double)]]] =
    zRange(client, k, from, to, t) map { r =>
      r.left.toOption.map(_.asTuples).filter(_.nonEmpty)
    }

  def zIncrBy(client: redis.Client, k: ChannelBuffer, by: Double, m: ChannelBuffer) =
    client.zIncrBy(k, by, m)

  def zScore(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer) =
    client.zScore(k, v)

  def zAdd(client: redis.Client, k: ChannelBuffer, s: Double, m: ChannelBuffer) =
    client.zAdd(k, s, m)

  def zRem(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.zRem(k, vs)

}