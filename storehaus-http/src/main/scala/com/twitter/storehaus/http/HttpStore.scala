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
import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }
import org.jboss.netty.handler.codec.http.{ HttpRequest, HttpResponse, DefaultHttpRequest, HttpVersion, HttpMethod, HttpHeaders, HttpResponseStatus }
import com.twitter.util.Future
import com.twitter.finagle.{ Service, Http }
import com.twitter.storehaus.Store

object HttpException {
  def apply(response: HttpResponse): HttpException = 
    new HttpException(response.getStatus.getCode, response.getStatus.getReasonPhrase, response.getContent.toString(Charset.forName("UTF-8")))
}

case class HttpException(code: Int, reasonPhrase: String, content: String) extends Exception(reasonPhrase + Option(content).map("\n" + _ ).getOrElse(""))

object HttpStore {
  def apply(dest: String): HttpStore = new HttpStore(Http.newService(dest))
}

class HttpStore(val client: Service[HttpRequest, HttpResponse]) extends Store[String, ChannelBuffer] {
  override def get(k: String): Future[Option[ChannelBuffer]] = {
    val request =  new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, k)
    request.headers.set(HttpHeaders.Names.CONTENT_LENGTH, "0")
    client(request).map{ response => 
      response.getStatus match {
        case HttpResponseStatus.OK => Some(response.getContent)
        case HttpResponseStatus.NOT_FOUND => None
        case _ => throw HttpException(response)
      }
    }
  }

  override def put(kv: (String, Option[ChannelBuffer])): Future[Unit] = {
    val request = kv match {
      case (k, Some(cb)) =>
        val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, k)
        req.setContent(cb)
        req.headers.set(HttpHeaders.Names.CONTENT_LENGTH, cb.readableBytes.toString)
        req
      case (k, None) =>
        val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, k)
        req.headers.set(HttpHeaders.Names.CONTENT_LENGTH, "0")
        req
    }
    client(request).map{ response =>
      response.getStatus match {
        case HttpResponseStatus.OK => ()
        case HttpResponseStatus.CREATED => ()
        case HttpResponseStatus.NO_CONTENT => ()
        case _ => throw HttpException(response)
      }
    }
  }
}

object HttpStringStore {
  def apply(dest: String): HttpStringStore = new HttpStringStore(Http.newService(dest))
}

class HttpStringStore(val client: Service[HttpRequest, HttpResponse]) extends Store[String, String] {
  private val store = new HttpStore(client)
  private val utf8 = Charset.forName("UTF-8")

  override def get(k: String): Future[Option[String]] = store.get(k).map(_.map(_.toString(utf8)))

  override def put(kv: (String, Option[String])): Future[Unit] = store.put((
    kv._1,
    kv._2.map(s => ChannelBuffers.wrappedBuffer(s.getBytes(utf8)))
  ))
}
