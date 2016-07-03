/*
 * Copyright 2013 Twitter Inc.
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

/** A type to represent how Seq of futures are collected into a future of Seq[T] */
trait FutureCollector extends java.io.Serializable {
  def apply[T](in: Seq[Future[T]]): Future[Seq[T]]
}

/** Some factory methods and instances of FutureCollector that are used in storehaus */
object FutureCollector {
  /**
   * If any future fails, the remaining future fails.
   */
  implicit def default: FutureCollector = new FutureCollector {
    override def apply[T](futureSeq: Seq[Future[T]]) = Future.collect(futureSeq)
  }

  /** All failing futures are filtered during collection.  */
  def bestEffort: FutureCollector = new FutureCollector {
    override def apply[T](futureSeq: Seq[Future[T]]) =
      Future.collect {
        futureSeq.map { f: Future[T] =>
          f.map { Some(_) }.handle { case x: Exception => None } // don't eat Error
        }
      }.map { _.flatten }
  }
}
