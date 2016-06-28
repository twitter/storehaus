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

package com.twitter.storehaus

import com.twitter.util.Future

import scala.util.{Failure, Success, Try}

package object mysql {
  def toTwitterFuture[T](t: Try[T]): Future[T] = t match {
    case Success(v) => Future.value(v)
    case Failure(ex) => Future.exception(ex)
  }
  def toTwitterFuture[T](ts: Seq[Try[T]]): Future[Seq[T]] = {
    val builder = Vector.newBuilder[T]
    val res = ts.iterator.foldLeft(Try(())) {
      case (f: Failure[_], _) => f
      case (s@Success(_), Success(t)) => builder += t; s
      case (_, Failure(e)) => Failure(e)
    }.map(_ => builder.result())
    toTwitterFuture(res)
  }
}
