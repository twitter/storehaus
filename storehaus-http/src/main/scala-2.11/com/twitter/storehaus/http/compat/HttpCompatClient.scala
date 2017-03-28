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

import com.twitter.finagle.http.{ Fields, Method, Request, Response, Status }
import com.twitter.finagle.netty3.{ BufChannelBuffer, ChannelBufferBuf }
import org.jboss.netty.buffer.ChannelBuffer

object HttpCompatClient {

  val OK = Status.Ok

  val NOT_FOUND = Status.NotFound

  val CREATED = Status.Created

  val NO_CONTENT = Status.NoContent

  val METHOD_NOT_ALLOWED = Status.MethodNotAllowed

  val CONTENT_LENGTH = Fields.ContentLength

  val GET = Method.Get

  val PUT = Method.Put

  val DELETE = Method.Delete

  def getStatus(rep: Response) =
    rep.status

  def getStatusCode(rep: Response) =
    rep.statusCode

  def getStatusReasonPhrase(rep: Response) =
    rep.status.reason

  def getContent(rep: Response) =
    BufChannelBuffer(rep.content)

  def getContent(req: Request) =
    BufChannelBuffer(req.content)

  def setContent(req: Request, cb: ChannelBuffer) =
    req.content = new ChannelBufferBuf(cb)

  def setContent(rep: Response, cb: ChannelBuffer) =
    rep.content = new ChannelBufferBuf(cb)

  def getContentString(rep: Response) =
    rep.contentString

  def getContentString(req: Request) =
    req.contentString

  def getContentReadableBytes(req: Request) =
    getContent(req).readableBytes

  def getMethod(req: Request) =
    req.method

  def getUri(req: Request) =
    req.uri

  def getDefaultHttpResponseForRequest(req: Request, status: Status) = {
    val res = Response(req)
    res.status = status
    res
  }

  def getDefaultHttpGetRequestForUrl(url: String) =
    Request(url)

  def getDefaultHttpPutRequestForUrl(url: String) =
    Request(Method.Put, url)

  def getDefaultHttpDeleteRequestForUrl(url: String) =
    Request(Method.Delete, url)

  def setHeader(req: Request, name: String, value: String) =
    req.headerMap.set(name, value)

  def setHeader(rep: Response, name: String, value: String) =
    rep.headerMap.set(name, value)


}