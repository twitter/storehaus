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

/** A writable Counter useful for documenting the number
 *  of times a target operation occurs
 */
trait Counter {
  def incr(delta: Long = 1L): Unit
}

/** A Stat provides a way to export data in time-series format.
 *  An example use case would be timing of operations
 */
trait Stat {
  def add(value: Float)
}

/** A Instrumentation implements a writable interface exporting a set of
 *  primatives for capturing diagnostic information
 */
trait Instrumentation {
  /** Resolves a Counter identified by set of Strings */
  def counter(name: String*): Counter

  /** Resolves a State identified by a set of Strings */
  def stat(name: String*): Stat

  /** Records the execution time of an arbitrary function in
   *  the provided time unit attached to the provided stat */
  def time[T](
    timeUnit: TimeUnit, stat: Stat)
    (f: => T): T = {
    val elapsed = Stopwatch.start()
    val result = f
    stat.add(elapsed().inUnit(timeUnit))
    result
  }

  /** Records the execution time of an arbitrary function
   *  in the provided time unit to a Stat identified by
   *  the provided Seq of Strings */
  def time[T](
    timeUnit: TimeUnit, name: String*)
    (f: => T): T =
    time(timeUnit, stat(name: _*))(f)

  /** Records the execution time of an arbitrary function in milliseconds
   *  to a Stat identified by the provided Seq of Strings */
  def time[T](
    name: String*)
    (f: => T): T =
    time(TimeUnit.MILLISECONDS, name: _*)(f)

  /** Asyncrhonously records the execution time of a function
   *  producing a Future in the provided time unit to the provided Stat */
  def timeFuture[T](
    timeUnit: TimeUnit, stat: Stat)
    (f: => Future[T]): Future[T] = {
    val elapsed = Stopwatch.start()
    f ensure {
      stat.add(elapsed().inUnit(timeUnit))
    }
  }

  /** Asyncronously records the execution time of a function
   *  producing a Future in the provided time unit to a Stat
   *  identified by the provided Seq of Strings */
  def timeFuture[T](
    timeUnit: TimeUnit, name: String*)
    (f: => Future[T]): Future[T] =
    timeFuture(timeUnit, stat(name: _*))(f)

  /** Asyncronously records the execution time of a function producing
   *  a Future in milliseconds to a Stat identified by the provided
   *  Seq of Strings */
  def timeFuture[T](
    name: String*)
    (f: => Future[T]): Future[T] =
    timeFuture(TimeUnit.MILLISECONDS, name: _*)(f)

  /** Returns an Instrumentation which prefixes all
   *  Stat and Counter paths with the provided name */
  def prefix(name: String): Instrumentation =
    new NameTranslatingInstrumentation(this) {
      private val seqPrefix = Seq(name)
      protected [this] def translate(name: Seq[String]) =
        seqPrefix ++ name
    }

  /** Returns an Instrumentation which applies A suffix
   *  to all Stat and Counter paths with the provided name */
  def suffix(name: String): Instrumentation =
    new NameTranslatingInstrumentation(this) {
      private val seqSuffix = Seq(name)
      protected [this] def translate(name: Seq[String]) =
        name ++ seqSuffix
    }

  /** Produces an Instrumentation from a provided Instrumentation
   *  which translates all stat and counter lookups with some path
   *  augmentation */
  abstract class NameTranslatingInstrumentation(self: Instrumentation)
    extends Instrumentation {
    protected def translate(name: Seq[String]): Seq[String]
    def counter(name: String*) = self.counter(translate(name): _*)
    def stat(name: String*)    = self.stat(translate(name): _*)
  }
}
