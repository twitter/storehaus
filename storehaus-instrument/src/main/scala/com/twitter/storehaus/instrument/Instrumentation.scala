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

package com.twitter.storehaus.instrument

import com.twitter.util.{ Future, Stopwatch }
import java.util.concurrent.TimeUnit

trait Counter {
  def incr(delta: Long = 1L): Unit
}

trait Stat {
  def add(value: Float)
}

trait Instrumentation {

  def time[T](
    unit: TimeUnit, stat: Stat)
    (f: => T): T = {
    val elapsed = Stopwatch.start()
    val result = f
    stat.add(elapsed().inUnit(unit))
    result
  }

  def time[T](
    unit: TimeUnit, name: String*)
    (f: => T): T =
    time(unit, stat(name: _*))(f)

  def time[T](
    name: String*)
    (f: => T): T =
    time(TimeUnit.MILLISECONDS, name: _*)(f)

  def timeFuture[T](
    unit: TimeUnit, stat: Stat)
    (f: => Future[T]): Future[T] = {
    val elapsed = Stopwatch.start()
    f ensure {
      stat.add(elapsed().inUnit(unit))
    }
  }

  def timeFuture[T](
    unit: TimeUnit, name: String*)
    (f: => Future[T]): Future[T] =
    timeFuture(unit, stat(name: _*))(f)

  def timeFuture[T](
    name: String*)
    (f: => Future[T]): Future[T] =
    timeFuture(TimeUnit.MILLISECONDS, name: _*)(f)
  
  def counter(name: String*): Counter

  def stat(name: String*): Stat

  def prefix(name: String): Instrumentation =
    new NameTranslatingInstrumentation(this) {
      private val seqPrefix = Seq(name)
      protected [this] def translate(name: Seq[String]) =
        seqPrefix ++ name
    }

  def suffix(name: String): Instrumentation =
    new NameTranslatingInstrumentation(this) {
      private val seqSuffix = Seq(name)
      protected [this] def translate(name: Seq[String]) =
        name ++ seqSuffix
    }

  abstract class NameTranslatingInstrumentation(self: Instrumentation)
    extends Instrumentation {
    protected def translate(name: Seq[String]): Seq[String]
    def counter(name: String*) = self.counter(translate(name): _*)
    def stat(name: String*)    = self.stat(translate(name): _*)
  }
}
