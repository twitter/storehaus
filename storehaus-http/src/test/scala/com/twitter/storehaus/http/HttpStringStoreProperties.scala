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
import com.twitter.finagle.http.compat.NettyAdaptor
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{ HttpRequest, HttpResponse, DefaultHttpResponse, HttpResponseStatus, HttpMethod, HttpHeaders }
import com.twitter.util.{ Await, Future }
import com.twitter.finagle.{ Service, Http, ListeningServer }
import com.twitter.storehaus.{ FutureOps, Store }
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import org.scalacheck.{ Arbitrary, Gen, Properties }
import org.scalacheck.Prop._

object HttpStringStoreProperties extends Properties("HttpStringStore") with CloseableCleanup[ListeningServer] {
  def validPairs: Gen[List[(String, Option[String])]] =
    NonEmpty.Pairing.alphaStrs().map(_.map{ case (k, v) => ("/" + k, v) })

  def baseTest[K: Arbitrary, V: Arbitrary : Equiv](store: Store[K, V], validPairs: Gen[List[(K, Option[V])]])
                                                  (put: (Store[K, V], List[(K, Option[V])]) => Unit) =
    forAll(validPairs) { (examples: List[(K, Option[V])]) =>
      put(store, examples)
      examples.toMap.forall { case (k, optV) =>
        val res = Await.result(store.get(k))
        Equiv[Option[V]].equiv(res, optV)
      }
    }

  def putStoreTest[K: Arbitrary, V: Arbitrary : Equiv](store: Store[K, V], validPairs: Gen[List[(K, Option[V])]]) =
    baseTest(store, validPairs) { (s, pairs) =>
      pairs.foreach {
        case (k, v) =>
          Await.result(s.put((k, v)))
      }
    }

  def multiPutStoreTest[K: Arbitrary, V: Arbitrary : Equiv](store: Store[K, V], validPairs: Gen[List[(K, Option[V])]]) =
    baseTest(store, validPairs) { (s, pairs) => 
      Await.result(FutureOps.mapCollect(s.multiPut(pairs.toMap)))
    }

  def storeTest(store: Store[String, String]) =
    putStoreTest(store, validPairs) && multiPutStoreTest(store, validPairs)

  val service = new Service[HttpRequest, HttpResponse] {
    private val map = new ConcurrentHashMap[String, String]()
    private val utf8 = Charset.forName("UTF-8")

    def apply(request: HttpRequest): Future[HttpResponse] = {
      val response = request.getMethod match {
        case HttpMethod.GET =>
          Option(map.get(request.getUri)).map{ v =>
            val resp = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.OK)
            val content = ChannelBuffers.wrappedBuffer(v.getBytes(utf8))
            resp.setContent(content)
            resp.headers.set(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes.toString)
            resp
          }.getOrElse {
            val resp = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.NOT_FOUND)
            resp.headers.set(HttpHeaders.Names.CONTENT_LENGTH, "0")
            resp
          }
        case HttpMethod.DELETE =>
          map.remove(request.getUri)
          val resp = new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.NO_CONTENT)
          resp.headers.set(HttpHeaders.Names.CONTENT_LENGTH, "0")
          resp
        case HttpMethod.PUT =>
          val maybeOldV = Option(map.put(request.getUri, request.getContent.toString(utf8)))
          val resp = new DefaultHttpResponse(request.getProtocolVersion, maybeOldV.map(_ => HttpResponseStatus.OK).getOrElse(HttpResponseStatus.CREATED))
          resp.setContent(request.getContent)
          resp.headers.set(HttpHeaders.Names.CONTENT_LENGTH, request.getContent.readableBytes.toString)
          resp
        case _ =>
          new DefaultHttpResponse(request.getProtocolVersion, HttpResponseStatus.METHOD_NOT_ALLOWED)
      }
      Future.value(response)
    }
  }

  val server = Http.serve("localhost:0", NettyAdaptor andThen service)

  val store = HttpStringStore(server.boundAddress.toString.substring(1)) // i dont know how else to convert boundAddress into something usable

  property("HttpStringStore test") = storeTest(store)

  override def closeable = server

  override def cleanup() = {
    println("closing server")
    super.cleanup()
  }
}
