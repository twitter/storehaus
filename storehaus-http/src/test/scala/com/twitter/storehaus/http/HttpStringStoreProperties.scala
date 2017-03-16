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

import java.util.concurrent.ConcurrentHashMap
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{ DefaultHttpResponse,
  HttpResponseStatus, HttpMethod, HttpHeaders }
import com.twitter.util.{ Await, Future }
import com.twitter.finagle.{ Service, Http, ListeningServer }
import com.twitter.storehaus.http.compat.{ HttpCompatClient, HttpRequest, HttpResponse, NettyAdaptor }
import com.twitter.storehaus.{ FutureOps, Store }
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import org.scalacheck.{Prop, Arbitrary, Gen, Properties}
import org.scalacheck.Prop._

object HttpStringStoreProperties
    extends Properties("HttpStringStore") with CloseableCleanup[ListeningServer] {
  def validPairs: Gen[List[(String, Option[String])]] =
    NonEmpty.Pairing.alphaStrs().map(_.map{ case (k, v) => ("/" + k, v) })

  def baseTest[K: Arbitrary, V: Arbitrary : Equiv](
      store: Store[K, V], validPairs: Gen[List[(K, Option[V])]])
      (put: (Store[K, V], List[(K, Option[V])]) => Unit): Prop =
    forAll(validPairs) { (examples: List[(K, Option[V])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        val res = Await.result(store.get(k))
        Equiv[Option[V]].equiv(res, optV)
      }
    }

  def putStoreTest[K: Arbitrary, V: Arbitrary : Equiv](
      store: Store[K, V], validPairs: Gen[List[(K, Option[V])]]): Prop =
    baseTest(store, validPairs) { (s, pairs) =>
      pairs.foreach {
        case (k, v) =>
          Await.result(s.put((k, v)))
      }
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary : Equiv](
      store: Store[K, V], validPairs: Gen[List[(K, Option[V])]]): Prop =
    baseTest(store, validPairs) { (s, pairs) =>
      Await.result(FutureOps.mapCollect(s.multiPut(pairs.toMap)))
    }

  def storeTest(store: Store[String, String]): Prop =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val service = new Service[HttpRequest, HttpResponse] {
    private val map = new ConcurrentHashMap[String, String]()
    private val utf8 = Charset.forName("UTF-8")

    def apply(request: HttpRequest): Future[HttpResponse] = {
      val response = HttpCompatClient.getMethod(request) match {
        case HttpCompatClient.GET =>
          Option(map.get(HttpCompatClient.getUri(request))).map{ v =>
            val resp = HttpCompatClient.getDefaultHttpResponseForRequest(request, HttpCompatClient.OK)
            val content = ChannelBuffers.wrappedBuffer(v.getBytes(utf8))
            HttpCompatClient.setContent(resp, content)
            HttpCompatClient.setHeader(resp, HttpCompatClient.CONTENT_LENGTH, content.readableBytes.toString)
            resp
          }.getOrElse {
            val resp = HttpCompatClient.getDefaultHttpResponseForRequest(request, HttpCompatClient.NOT_FOUND)
            HttpCompatClient.setHeader(resp, HttpCompatClient.CONTENT_LENGTH, "0")
            resp
          }
        case HttpCompatClient.DELETE =>
          map.remove(HttpCompatClient.getUri(request))
          val resp = HttpCompatClient.getDefaultHttpResponseForRequest(request, HttpCompatClient.NO_CONTENT)
          HttpCompatClient.setHeader(resp, HttpCompatClient.CONTENT_LENGTH, "0")
          resp
        case HttpCompatClient.PUT =>
          val maybeOldV = Option(map.put(HttpCompatClient.getUri(request), HttpCompatClient.getContentString(request)))
          val resp = HttpCompatClient.getDefaultHttpResponseForRequest(request,
            maybeOldV.map(_ => HttpCompatClient.OK).getOrElse(HttpCompatClient.CREATED))
          HttpCompatClient.setContent(resp, HttpCompatClient.getContent(request))
          HttpCompatClient.setHeader(resp, HttpCompatClient.CONTENT_LENGTH, HttpCompatClient.getContentReadableBytes(request).toString)
          resp
        case _ =>
          HttpCompatClient.getDefaultHttpResponseForRequest(request, HttpCompatClient.METHOD_NOT_ALLOWED)
      }
      Future.value(response)
    }
  }

  val server = Http.serve("localhost:0", NettyAdaptor andThen service)

  // i dont know how else to convert boundAddress into something usable
  val store = HttpStringStore(server.boundAddress.toString.substring(1))

  property("HttpStringStore test") = storeTest(store)

  override def closeable: ListeningServer = server

  override def cleanup(): Unit = {
    super.cleanup()
  }
}
