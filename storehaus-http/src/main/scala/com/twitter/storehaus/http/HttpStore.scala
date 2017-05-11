/*
 * Copyright 2014 Twitter Inc.
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

package com.twitter.storehaus.http

import java.nio.charset.Charset

import com.twitter.bijection.{Bijection, StringCodec}
import com.twitter.finagle.http._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.{Http, Service}
import com.twitter.io.Buf
import com.twitter.storehaus.{ConvertedStore, Store}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

object HttpException {
  def apply(response: Response): HttpException = {
    new HttpException(response.status.code, response.status.reason,
      response.contentString)
  }
}

case class HttpException(code: Int, reasonPhrase: String, content: String)
  extends Exception(reasonPhrase + Option(content).map("\n" + _ ).getOrElse(""))

object HttpStore {
  def apply(dest: String): HttpStore = new HttpStore(Http.newService(dest))

  // Here for backward compatibility, you probably don't want to use it in new code
  def toChannelBufferStore(store: Store[Buf, Buf]): Store[String, ChannelBuffer] = {
    implicit val bij = ChannelBuffer2BufBijection
    store.convert(Buf.Utf8(_))
  }
}

object ChannelBuffer2BufBijection extends Bijection[ChannelBuffer, Buf] {
  def apply(c: ChannelBuffer): Buf = ChannelBufferBuf.newOwned(c)
  override def invert(m: Buf): ChannelBuffer = ChannelBufferBuf.Owned.extract(m)
}

class HttpStore(val client: Service[Request, Response])
    extends Store[String, Buf] {

  override def get(k: String): Future[Option[Buf]] = {
    val request = Request(Method.Get, k)
    request.contentLength = 0
    client(request).map{ response =>
      response.status match {
        case Status.Ok => Some(response.content)
        case Status.NotFound => None
        case _ => throw HttpException(response)
      }
    }
  }

  override def put(kv: (String, Option[Buf])): Future[Unit] = {
    val request = kv match {
      case (k, Some(buf)) =>
        val req = Request(Method.Put, k)
        req.content = buf
        req.contentLength = buf.length
        req
      case (k, None) =>
        val req = Request(Method.Delete, k)
        req.contentLength = 0
        req
    }
    client(request).map{ response =>
      response.status match {
        case Status.Ok => ()
        case Status.Created => ()
        case Status.NoContent => ()
        case _ => throw HttpException(response)
      }
    }
  }
}

object BufBijection extends Bijection[Buf, Array[Byte]] {
  override def apply(buf: Buf) = Buf.ByteArray.Owned.extract(buf)
  override def invert(ary: Array[Byte]) = Buf.ByteArray.Owned(ary)
}

object HttpStringStore {
  def apply(dest: String): HttpStringStore =
    new HttpStringStore(Http.newService(dest))
}

class HttpStringStore(val client: Service[Request, Response])
    extends ConvertedStore[String, String, Buf, String](
      new HttpStore(client))(identity)(StringCodec.utf8 andThen BufBijection.inverse)
