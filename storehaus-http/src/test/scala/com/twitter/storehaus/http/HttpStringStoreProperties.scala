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
import java.util.concurrent.ConcurrentHashMap

import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.finagle.{Http, ListeningServer, Service}
import com.twitter.storehaus.testing.CloseableCleanup
import com.twitter.storehaus.testing.generator.NonEmpty
import com.twitter.storehaus.{FutureOps, Store}
import com.twitter.util.{Await, Future}
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

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

  val service = new Service[Request, Response] {
    private val map = new ConcurrentHashMap[String, String]()
    private val utf8 = Charset.forName("UTF-8")

    def apply(request: Request): Future[Response] = {
      val response = request.method match {
        case Method.Get =>
          Option(map.get(request.uri)).map{ v =>
            val resp = Response(request.version, Status.Ok)
            resp.contentString = v
            resp.contentLength = v.getBytes.size
            resp
          }.getOrElse {
            val resp = Response(request.version, Status.NotFound)
            resp.contentLength = 0
            resp
          }
        case Method.Delete =>
          map.remove(request.uri)
          val resp = Response(request.version, Status.NoContent)
          resp.contentLength = 0
          resp
        case Method.Put =>
          val maybeOldV = Option(map.put(request.uri, request.contentString))
          val resp = Response(request.version,
            maybeOldV.map(_ => Status.Ok).getOrElse(Status.Created))
          resp.content = request.content
          resp.contentLength = request.content.length
          resp
        case _ =>
          Response(request.version, Status.MethodNotAllowed)
      }
      Future.value(response)
    }
  }

  val server = Http.serve("localhost:0", service)

  // i dont know how else to convert boundAddress into something usable
  val store = HttpStringStore(server.boundAddress.toString.substring(1))

  property("HttpStringStore test") = storeTest(store)

  override def closeable: ListeningServer = server

  override def cleanup(): Unit = {
    super.cleanup()
  }
}
