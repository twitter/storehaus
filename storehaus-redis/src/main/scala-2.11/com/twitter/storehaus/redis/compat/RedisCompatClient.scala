package com.twitter.storehaus.redis.compat

import scala.util.Either

import com.twitter.finagle.redis
import com.twitter.finagle.redis.protocol.ZRangeResults
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.io.Buf
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

object RedisCompatClient {

  def get(client: redis.Client, k: ChannelBuffer): Future[Option[ChannelBuffer]] =
    client.get(new ChannelBufferBuf(k)) map {
      case Some(v) => Some(BufChannelBuffer(v))
      case _ => None
    }

  def mGet2(client: redis.Client, ks: Seq[ChannelBuffer]): Future[Seq[Option[ChannelBuffer]]] =
    client.mGet(ks map (new ChannelBufferBuf(_))) map { vs =>
      vs map {
        case Some(v) => Some(BufChannelBuffer(v))
        case None => None
      }
    }

  def set(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer): Future[Unit] =
    client.set(new ChannelBufferBuf(k), new ChannelBufferBuf(v))

  def setEx(client: redis.Client, k: ChannelBuffer, t: Long, v: ChannelBuffer): Future[Unit] =
    client.setEx(new ChannelBufferBuf(k), t, new ChannelBufferBuf(v))

  def hGetAll(client: redis.Client, k: ChannelBuffer): Future[Seq[(ChannelBuffer, ChannelBuffer)]] =
    client.hGetAll(new ChannelBufferBuf(k)) map { m =>
      m map { case (f, v) => (BufChannelBuffer(f), BufChannelBuffer(v)) }
    }

  def hMSet(client: redis.Client, k: ChannelBuffer, m: Map[ChannelBuffer, ChannelBuffer]): Future[Unit] = {
    val mBuf: Map[Buf, Buf] = (m.toSeq map { case (f, v) => (new ChannelBufferBuf(f), new ChannelBufferBuf(v)) }).toMap
    client.hMSet(new ChannelBufferBuf(k), mBuf)
  }

  def expire(client: redis.Client, k: ChannelBuffer, t: Int) =
    client.expire(new ChannelBufferBuf(k), t)

  def del(client: redis.Client, ks: Seq[ChannelBuffer]) =
    client.dels(ks map (new ChannelBufferBuf(_)))

  def incrBy(client: redis.Client, k: ChannelBuffer, by: Long) =
    client.incrBy(new ChannelBufferBuf(k), by)

  def sMembers(client: redis.Client, k: ChannelBuffer): Future[Set[ChannelBuffer]] =
    client.sMembers(new ChannelBufferBuf(k)) map { s =>
      s.map(BufChannelBuffer(_))
    }

  def sAdd(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.sAdd(new ChannelBufferBuf(k), vs map (new ChannelBufferBuf(_)))

  def sRem(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.sRem(new ChannelBufferBuf(k), vs map (new ChannelBufferBuf(_)))

  def sIsMember(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer) =
    client.sIsMember(new ChannelBufferBuf(k), new ChannelBufferBuf(v))

  def zRange(client: redis.Client, k: ChannelBuffer, from: Long, to: Long, t: Boolean): Future[Either[ZRangeResults,Seq[ChannelBuffer]]] =
    client.zRange(new ChannelBufferBuf(k), from, to, t) map {
      case Left(zRangeResults) => Left(zRangeResults)
      case Right(vs) => Right(vs map (BufChannelBuffer(_)))
    }

  def zRangeLeftAsTuples(client: redis.Client, k: ChannelBuffer, from: Long, to: Long, t: Boolean): Future[Option[Seq[(ChannelBuffer, Double)]]] =
    zRange(client, k, from, to, t) map { r =>
      r.left.toOption.map(_.asTuples).filter(_.nonEmpty) map { ts =>
        ts map {
          case (t, d) => (BufChannelBuffer(t), d)
        }
      }
    }

  def zIncrBy(client: redis.Client, k: ChannelBuffer, by: Double, m: ChannelBuffer) =
    client.zIncrBy(new ChannelBufferBuf(k), by, new ChannelBufferBuf(m))

  def zScore(client: redis.Client, k: ChannelBuffer, v: ChannelBuffer) =
    client.zScore(new ChannelBufferBuf(k), new ChannelBufferBuf(v))

  def zAdd(client: redis.Client, k: ChannelBuffer, s: Double, m: ChannelBuffer) =
    client.zAdd(new ChannelBufferBuf(k), s, new ChannelBufferBuf(m))

  def zRem(client: redis.Client, k: ChannelBuffer, vs: List[ChannelBuffer]) =
    client.zRem(new ChannelBufferBuf(k), vs map (new ChannelBufferBuf(_)))

}