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

package com.twitter.storehaus.algebra

import com.twitter.algebird.{ Semigroup, Monoid, Group, CMSHash  }
import com.twitter.storehaus.{StoreProxy, Store}
import com.twitter.util.Future

import scala.collection.mutable.ListBuffer



// The update frequency is how often we should update the mutable CMS
// other steps will just query the pre-established HH's
// This will only kick in after the first 1000 tuples since a Roll Over
case class UpdateFrequency(toInt: Int)

// This is after how many entries we will reset the CMS
// This is to account for temporal changes in the HH's
case class RollOverFrequency(toLong: Long)

// The heavy hitters percent is used to control above what % of items we should send to the backing
// aggregator
case class HeavyHittersPercent(toFloat: Float)


class ApproxHHTracker[K](hhPct: HeavyHittersPercent, updateFreq: UpdateFrequency, roFreq: RollOverFrequency) {
  private[this] final val WIDTH = 1000
  private[this] final val DEPTH = 4
  private[this] final val hh = new java.util.HashMap[K, Long]()
  private[this] final var totalCount = 0L
  private[this] final var hhMinReq = 0L
  private[this] final val hhPercent = hhPct.toFloat
  private[this] final val updateFrequency = updateFreq.toInt
  private[this] final val rollOverFrequency = roFreq.toLong
  private[this] final var countsTable = Array.fill(WIDTH * DEPTH)(0L)

  private[this] final val hashes: IndexedSeq[CMSHash] = {
    val r = new scala.util.Random(5)
    (0 until DEPTH).map { _ => CMSHash(r.nextInt, 0, WIDTH) }
  }.toIndexedSeq

  @inline
  private[this] final def frequencyEst(item : Long): Long = {
    var min = Long.MaxValue
    var indx = 0
    while (indx < DEPTH) {
      val newVal = countsTable(indx*WIDTH + hashes(indx)(item))
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
      val newItemCount = frequencyEst(itemHashCode) + 1L
      if (newItemCount >= hhMinReq) {
        hh.put(item, totalCount)
        pruneHH
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
  }
  // End of thread-unsafe update steps

  private[this] final val updateStep = new java.util.concurrent.atomic.AtomicLong(0L)

  final def hhFilter(t: K): Boolean = {
    // This is the entry point from the iterator into our CMS implementation
    // We only update on certain steps < a threshold and on every nTh step.

    // We only acquire locks/synchronize when hitting the update/write path.
    // most passes into this function will just hit the final line(containsKey).
    // which is our thread safe read path.
    val newCounter = updateStep.incrementAndGet
    if (newCounter > rollOverFrequency) {
      hh.synchronized {
        updateStep.set(1L)
        resetCMS
      }
    }
    if(newCounter < 1000L || newCounter % updateFrequency == 0L) {
      hh.synchronized {
        updateItem(t)
      }
    }
    hh.containsKey(t)
  }

  final def query(t: K): Boolean = hh.containsKey(t)

}


class HHFilteredStore[K, V](val self: Store[K, V]) extends StoreProxy[K, V] {
  private[this] val approxTracker = new ApproxHHTracker[K](HeavyHittersPercent(0.01f), UpdateFrequency(2), RollOverFrequency(10000000L))

  override def put(kv: (K, Option[V])): Future[Unit] = if(approxTracker.hhFilter(kv._1) || !kv._2.isDefined) self.put(kv) else Future.Unit

  override def get(k: K): Future[Option[V]] = if(approxTracker.query(k)) self.get(k) else Future.None

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val backed = self.multiGet(ks.filter(k => approxTracker.query(k)))
    ks.map { k: K1 => (k, backed.getOrElse(k, Future.None)) }(collection.breakOut)
  }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    val backed = self.multiPut(kvs.filter(kv => (!kv._2.isDefined || approxTracker.hhFilter(kv._1))))
    kvs.map { kv => (kv._1, backed.getOrElse(kv._1, Future.Unit)) }(collection.breakOut)
  }
}