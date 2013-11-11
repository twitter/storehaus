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

import com.twitter.storehaus.cache.MutableCache

/** Instrumentation backed by MutableCaches */
trait CacheStoreInstrumentation extends Instrumentation {
  def counters: MutableCache[Seq[String], Long]
  def stats: MutableCache[Seq[String], Seq[Float]]

  def counter(name: String*): Counter = {
    new Counter {
      def incr(delta: Long = 1L) {
        val oldValue = counters.get(name).getOrElse(0L)
        counters += (name, oldValue + delta)
      }
    }
  }

  def stat(name: String*): Stat = new Stat {
    def add(value: Float) {
      val oldValue = stats.get(name).getOrElse(Seq.empty)
      stats += (name, oldValue :+ value)
    }
  }

  def summary = {
    def formatName(name: Seq[String]) = name mkString "/"
    val counterLines =
      counters.iterator map { case (k, v) => (formatName(k), v.toString) }
    val statLines = stats.iterator collect {
      case (k, values) if !values.isEmpty =>
        val xs = values.sorted
        val n = xs.size
        val (min, med, max) = (xs(0), xs(n / 2), xs(n - 1))
        (formatName(k), "n=%d min=%.1f med=%.1f max=%.1f" format(n, min, med, max))
    }
    val sortedCounters = counterLines.toSeq sortBy(_._1)
    val sortedStats    = statLines.toSeq sortBy(_._1)
    val fmt = Function.tupled { (k: String, v: String) => "%-60s %s".format(k, v) }
    val fmtCounters = sortedCounters map fmt
    val fmtStats = sortedStats map fmt
    ("# counters\n" + (fmtCounters mkString "\n") +
     "\n# stats\n" + (fmtStats mkString "\n"))
  }
}
