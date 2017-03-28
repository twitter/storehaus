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
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.Future
import com.twitter.bijection.StringCodec
import com.twitter.bijection.netty.ChannelBufferBijection
import com.twitter.finagle.{ Service, Http }
import com.twitter.storehaus.http.compat.{ HttpCompatClient, HttpRequest, HttpResponse, NettyClientAdaptor }
import com.twitter.storehaus.{ Store, ConvertedStore }

object HttpException {
  def apply(response: HttpResponse): HttpException =
    new HttpException(HttpCompatClient.getStatusCode(response), HttpCompatClient.getStatusReasonPhrase(response),
      HttpCompatClient.getContentString(response))
}

case class HttpException(code: Int, reasonPhrase: String, content: String)
  extends Exception(reasonPhrase + Option(content).map("\n" + _ ).getOrElse(""))

object HttpStore {
  def apply(dest: String): HttpStore =
    new HttpStore(NettyClientAdaptor andThen Http.newService(dest))
}

class HttpStore(val client: Service[HttpRequest, HttpResponse])
    extends Store[String, ChannelBuffer] {
  override def get(k: String): Future[Option[ChannelBuffer]] = {
    val request = HttpCompatClient.getDefaultHttpGetRequestForUrl(k)
    HttpCompatClient.setHeader(request, HttpCompatClient.CONTENT_LENGTH, "0")
    client(request).map{ response =>
      HttpCompatClient.getStatus(response) match {
        case HttpCompatClient.OK => Some(HttpCompatClient.getContent(response))
        case HttpCompatClient.NOT_FOUND => None
        case _ => throw HttpException(response)
      }
    }
  }

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] = {
    val request = kv match {
      case (k, Some(cb)) =>
        val req = HttpCompatClient.getDefaultHttpPutRequestForUrl(k)
        HttpCompatClient.setContent(req, cb)
        HttpCompatClient.setHeader(req, HttpCompatClient.CONTENT_LENGTH, cb.readableBytes.toString)
        req
      case (k, None) =>
        val req = HttpCompatClient.getDefaultHttpDeleteRequestForUrl(k)
        HttpCompatClient.setHeader(req, HttpCompatClient.CONTENT_LENGTH, "0")
        req
    }
    client(request).map{ response =>
      HttpCompatClient.getStatus(response) match {
        case HttpCompatClient.OK => ()
        case HttpCompatClient.CREATED => ()
        case HttpCompatClient.NO_CONTENT => ()
        case _ => throw HttpException(response)
      }
    }
  }
}

object HttpStringStore {
  def apply(dest: String): HttpStringStore =
    new HttpStringStore(NettyClientAdaptor andThen Http.newService(dest))
}

class HttpStringStore(val client: Service[HttpRequest, HttpResponse])
    extends ConvertedStore[String, String, ChannelBuffer, String](
      new HttpStore(client))(identity)(StringCodec.utf8 andThen ChannelBufferBijection.inverse)
