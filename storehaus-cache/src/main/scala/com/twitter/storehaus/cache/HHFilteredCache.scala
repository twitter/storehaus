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

package com.twitter.storehaus.cache

import com.twitter.algebird.{ Semigroup, Monoid, Group, CMSHash  }
import com.twitter.util.Future


// The update frequency is how often we should update the mutable CMS
case class WriteOperationUpdateFrequency(toInt: Int)
object WriteOperationUpdateFrequency {
  def default = WriteOperationUpdateFrequency(100) // update 1% of the time
}

// This is how often in MS to roll over the CMS
case class RollOverFrequencyMS(toLong: Long)

object RollOverFrequencyMS {
  def default = RollOverFrequencyMS(3600 * 1000L) // 1 Hour
}

// The heavy hitters percent is used to control above what % of items we should send to the backing
// aggregator
case class HeavyHittersPercent(toFloat: Float)
object HeavyHittersPercent {
  def default = HeavyHittersPercent(0.001f) // 0.1% of the time
}

sealed class ApproxHHTracker[K](hhPct: HeavyHittersPercent, updateFreq: WriteOperationUpdateFrequency, roFreq: RollOverFrequencyMS) {
  private[this] final val WIDTH = 1000
  private[this] final val DEPTH = 4
  private[this] final val hh = new java.util.HashMap[K, Long]()
  private[this] final var totalCount = 0L
  private[this] final var hhMinReq = 0L
  private[this] final val hhPercent = hhPct.toFloat
  private[this] final val updateOpsFrequency = updateFreq.toInt
  private[this] final val rollOverFrequency = roFreq.toLong
  private[this] var countsTable = Array.fill(WIDTH * DEPTH)(0L)
  private[this] var nextRollOver: Long = System.currentTimeMillis + roFreq.toLong
  private[this] final val updateOps = new java.util.concurrent.atomic.AtomicInteger(0)

  private[this] final val hashes: IndexedSeq[CMSHash] = {
    val r = new scala.util.Random(5)
    (0 until DEPTH).map { _ => CMSHash(r.nextInt, 0, WIDTH) }
  }.toIndexedSeq

  @inline
  private[this] final def frequencyEst(item : Long): Long = {
    var min = Long.MaxValue
    var indx = 0
    while (indx < DEPTH) {
      val tableIdx = indx*WIDTH + hashes(indx)(item)
      val newVal = countsTable(tableIdx)
      if(newVal < min) min = newVal
      indx += 1
    }
    min
  }


  // Update functions in the write path
  // a synchronized guard should be used around these
  // to ensure consistent updates to backing data structures
  @inline
  private[this] final def updateItem(item: K) {
    val itemHashCode = item.hashCode
    totalCount += 1L
    hhMinReq = (hhPercent * totalCount).toLong
    var indx = 0
    while (indx < DEPTH) {
      val offset = indx*WIDTH + hashes(indx)(itemHashCode)
      countsTable.update(offset, countsTable(offset) + 1L)
      indx += 1
    }

    updateHH(item, itemHashCode)
  }

  @inline
  private[this] final def updateHH(item : K, itemHashCode: Int) {
    @inline
    def pruneHH {
      val iter = hh.values.iterator
      while(iter.hasNext) {
        val n = iter.next
        if(n < hhMinReq) {
          iter.remove
        }
      }
    }

    if(hh.containsKey(item)) {
      val v = hh.get(item)
      val newItemCount =  v + 1L
      if (newItemCount < hhMinReq) {
        pruneHH
      } else {
        hh.put(item, newItemCount)
      }
    } else {
      val newItemCount = frequencyEst(itemHashCode) // Do not + 1 since we have done that before.
      if (newItemCount >= hhMinReq) {
        hh.put(item, newItemCount)
      }
    }
  }

  // We include the ability to reset the CMS so we can age our counters
  // over time
  private[this] def resetCMS {
    hh.clear
    totalCount = 0L
    hhMinReq = 0L
    countsTable = Array.fill(WIDTH * DEPTH)(0L)
    updateOps.set(1)
    nextRollOver = System.currentTimeMillis + roFreq.toLong
  }
  // End of thread-unsafe update steps


  final def getFilterFunc: K => Boolean = {
      val opsCntr = updateOps.incrementAndGet

    if(opsCntr < 100 || opsCntr % updateOpsFrequency == 0) {
      hh.synchronized {
        if(System.currentTimeMillis > nextRollOver) {
          resetCMS
        }
        {k: K =>
          updateItem(k)
          hh.containsKey(k)
        }
      }
    } else {
      {k: K =>
        hh.containsKey(k)
      }
    }
  }

  final def clear {
    hh.synchronized {
      resetCMS
    }
  }

  final def query(t: K): Boolean = hh.containsKey(t)
}

/*
  This is a store for using the CMS code above to only store/read values which are heavy hitters in the CMS
*/
class HHFilteredCache[K, V](val self: MutableCache[K, V],
                            hhPercent: HeavyHittersPercent = HeavyHittersPercent.default,
                            writeUpdateFreq: WriteOperationUpdateFrequency = WriteOperationUpdateFrequency.default,
                            rolloverFreq: RollOverFrequencyMS = RollOverFrequencyMS.default) extends MutableCacheProxy[K, V] {
  private[this] val approxTracker = new ApproxHHTracker[K](hhPercent, writeUpdateFreq, rolloverFreq)

  override def +=(kv: (K, V)): this.type  =
    if(approxTracker.getFilterFunc(kv._1)) {
      self += kv
      this
    } else {
      this
    }

  override def multiInsert(kvs: Map[K, V]): this.type = {
    val filterFunc = approxTracker.getFilterFunc
    val backed = self.multiInsert(kvs.filterKeys(t => filterFunc(t)))
    this
  }

  override def hit(k: K): Option[V]  =
    if(approxTracker.getFilterFunc(k)) {
      self.hit(k)
    } else {
      None
    }

  override def clear: this.type = {
    self.clear
    approxTracker.clear
    this
  }

  override def iterator: Iterator[(K, V)] = {
    self.iterator.filter{kv => approxTracker.query(kv._1)}
  }

  override def contains(k: K): Boolean = if(approxTracker.query(k)) self.contains(k) else false
  override def get(k: K): Option[V] = if(approxTracker.query(k)) self.get(k) else None

}