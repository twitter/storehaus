
package com.twitter.storehaus.instrument

import com.twitter.storehaus.cache.MutableCache

/** Instrumentation backed by MutableCaches */
trait CacheStoreInstrumentation extends Instrumentation {
  def counters: MutableCache[Seq[String], Int]
  def stats: MutableCache[Seq[String], Seq[Float]]

  def counter(name: String*): Counter = {
    new Counter {
      def incr(delta: Int) {
        val oldValue = counters.get(name).getOrElse(0)
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
