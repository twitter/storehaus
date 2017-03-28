/*
 * Copyright 2016 Twitter Inc.
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

package com.twitter.storehaus.http.compat

import org.jboss.netty.handler.codec.http.{ DefaultHttpRequest => NettyDefaultHttpRequest}
import org.jboss.netty.handler.codec.http.{ DefaultHttpResponse => NettyDefaultHttpResponse}
import org.jboss.netty.handler.codec.http.{ HttpHeaders => NettyHttpHeaders }
import org.jboss.netty.handler.codec.http.{ HttpRequest => NettyHttpRequest }
import org.jboss.netty.handler.codec.http.{ HttpResponse => NettyHttpResponse }
import org.jboss.netty.handler.codec.http.{ HttpVersion => NettyHttpVersion }
import org.jboss.netty.handler.codec.http.{ HttpMethod => NettyHttpMethod }
import org.jboss.netty.handler.codec.http.{ HttpResponseStatus => NettyHttpResponseStatus }
import org.jboss.netty.buffer.{ ChannelBuffer => NettyChannelBuffer }
import java.nio.charset.Charset

object HttpCompatClient {

  val OK = NettyHttpResponseStatus.OK

  val CREATED = NettyHttpResponseStatus.CREATED

  val NOT_FOUND = NettyHttpResponseStatus.NOT_FOUND

  val NO_CONTENT = NettyHttpResponseStatus.NO_CONTENT

  val METHOD_NOT_ALLOWED = NettyHttpResponseStatus.METHOD_NOT_ALLOWED

  val CONTENT_LENGTH = NettyHttpHeaders.Names.CONTENT_LENGTH

  val GET = NettyHttpMethod.GET

  val PUT = NettyHttpMethod.PUT

  val DELETE = NettyHttpMethod.DELETE

  def getStatus(rep: NettyHttpResponse) =
    rep.getStatus

  def getStatusCode(rep: NettyHttpResponse) =
    rep.getStatus.getCode

  def getStatusReasonPhrase(rep: NettyHttpResponse) =
    rep.getStatus.getReasonPhrase

  def getContent(rep: NettyHttpResponse) =
    rep.getContent

  def getContent(req: NettyHttpRequest) =
    req.getContent

  def setContent(req: NettyHttpRequest, cb: NettyChannelBuffer) =
    req.setContent(cb)

  def setContent(rep: NettyHttpResponse, cb: NettyChannelBuffer) =
    rep.setContent(cb)

  def getContentString(rep: NettyHttpResponse) =
    rep.getContent.toString(Charset.forName("UTF-8"))

  def getContentString(req: NettyHttpRequest) =
    req.getContent.toString(Charset.forName("UTF-8"))

  def getContentReadableBytes(req: NettyHttpRequest) =
    getContent(req).readableBytes

  def getMethod(req: NettyHttpRequest) =
    req.getMethod

  def getUri(req: NettyHttpRequest) =
    req.getUri

  def getDefaultHttpResponseForRequest(req: NettyHttpRequest, status: NettyHttpResponseStatus) =
    new NettyDefaultHttpResponse(req.getProtocolVersion, status)

  def getDefaultHttpGetRequestForUrl(url: String) =
    new NettyDefaultHttpRequest(NettyHttpVersion.HTTP_1_1, NettyHttpMethod.GET, url)

  def getDefaultHttpPutRequestForUrl(url: String) =
    new NettyDefaultHttpRequest(NettyHttpVersion.HTTP_1_1, NettyHttpMethod.PUT, url)

  def getDefaultHttpDeleteRequestForUrl(url: String) =
    new NettyDefaultHttpRequest(NettyHttpVersion.HTTP_1_1, NettyHttpMethod.DELETE, url)

  def setHeader(req: NettyHttpRequest, name: String, value: String) =
    req.headers.set(name, value)

  def setHeader(rep: NettyHttpResponse, name: String, value: String) =
    rep.headers.set(name, value)

}