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

import scala.collection.SortedMap
import scala.annotation.tailrec

object Stack {
  def apply[K](maxSize:Int,
               backingIndexMap:SortedMap[CyclicIncrement[Int], K] = SortedMap.empty[CyclicIncrement[Int], K],
               backingKeyMap:Map[K, CyclicIncrement[Int]] = Map.empty[K, CyclicIncrement[Int]],
               cyclicIncrementProvider:CyclicIncrementProvider[Int] = CyclicIncrementProvider.intIncrementer): Stack[K] =
    new Stack[K](maxSize, backingIndexMap, backingKeyMap, cyclicIncrementProvider)
}

class Stack[K](maxSize:Int,
               backingIndexMap:SortedMap[CyclicIncrement[Int], K],
               backingKeyMap:Map[K, CyclicIncrement[Int]],
               cyclicIncrementProvider:CyclicIncrementProvider[Int]) {
  /**
   * Adds k to the top of the stack. If k is already in the Stack,
   * it will be put on the top. If k was not in the Stack and the
   * Stack was full, returns the evicted member.
   */
  def putOnTop(k: K): (Option[K], Stack[K]) = {
    val (newInc, newCyc) = cyclicIncrementProvider.getNewMaxIncrement 
    backingKeyMap.get(k) match {
      case Some(oldInc) => {
        val newBackingIndexMap = backingIndexMap - oldInc + (newInc->k)
        val newBackingKeyMap = backingKeyMap + (k->newInc)
        val newCyc2 = newCyc.cullOldIncrement(oldInc)
        (None, new Stack(maxSize, newBackingIndexMap, newBackingKeyMap, newCyc2))
      }
      case None => {
        if (isFull) {
          val (oldInc, lastK) = backingIndexMap.head
          val newBackingIndexMap = backingIndexMap - oldInc + (newInc->k)
          val newBackingKeyMap = backingKeyMap - lastK + (k->newInc)
          val newCyc2 = newCyc.cullOldIncrement(oldInc)
          (Some(lastK), new Stack(maxSize, newBackingIndexMap, newBackingKeyMap, newCyc2))
        } else {
          val newBackingIndexMap = backingIndexMap + (newInc->k)
          val newBackingKeyMap = backingKeyMap + (k->newInc)
          (None, new Stack(maxSize, newBackingIndexMap, newBackingKeyMap, newCyc))
        }
      }
    }
  }

  def dropOldest: (Option[K], Stack[K]) = {
    if (isEmpty) {
      (None, this)
    } else {
      val (lastInc, lastK) = backingIndexMap.head
      (Some(lastK), new Stack(maxSize, backingIndexMap - lastInc, backingKeyMap - lastK, cyclicIncrementProvider.cullOldIncrement(lastInc)))
    }
  }

  def remove(k: K): (Option[K], Stack[K]) = {
    backingKeyMap.get(k) match {
      case Some(inc) => (Some(k), new Stack(maxSize, backingIndexMap - inc, backingKeyMap - k, cyclicIncrementProvider.cullOldIncrement(inc)))
      case None => (None, this)
    }
  }

  def contains(k: K): Boolean = backingKeyMap.contains(k)

  def isOldest(k: K): Boolean = if (!isEmpty) { backingIndexMap.head._2 == k } else false

  def size = backingIndexMap.size

  def isFull = size >= maxSize

  def isEmpty = size <= 0

  def empty = new Stack[K](maxSize, backingIndexMap.empty, backingKeyMap.empty, cyclicIncrementProvider.empty)

  override def toString = backingIndexMap.map { _._2 }.mkString(",")
}

object LIRSStacks {
  def apply[K](sSize:Int, qSize:Int) = new LIRSStacks[K](Stack[K](sSize), Stack[K](qSize))
}

class LIRSStacks[K](stackS: Stack[K], stackQ: Stack[K]) {
  @tailrec
  final def prune: LIRSStacks[K] = {
    val (oldK, newStackS) = stackS.dropOldest
    oldK match {
      case Some(k) if stackQ.contains(k) => new LIRSStacks(newStackS, stackQ).prune
      case _ => this //We don't need to remove as there is either nothing to remove, or an LIR block is on the bottom
    }
  }

  def putOnTopOfS(k: K): (Option[K], LIRSStacks[K]) = {
    val (optK, newStackS) = stackS.putOnTop(k)
    (optK, new LIRSStacks(newStackS, stackQ))
  }

  def putOnTopOfQ(k: K): (Option[K], LIRSStacks[K]) = {
    val (optK, newStackQ) = stackQ.putOnTop(k)
    (optK, new LIRSStacks(stackS, newStackQ))
  }

  def dropOldestInS: (Option[K], LIRSStacks[K]) = {
    val (optK, newStackS) = stackS.dropOldest
    (optK, new LIRSStacks(newStackS, stackQ))
  }

  def dropOldestInQ: (Option[K], LIRSStacks[K]) = {
    val (optK, newStackQ) = stackQ.dropOldest
    (optK, new LIRSStacks(stackS, newStackQ))
  }

  def removeFromS(k: K): (Option[K], LIRSStacks[K]) = {
    val (optK, newStackS) = stackS.remove(k)
    (optK, new LIRSStacks(newStackS, stackQ))
  }

  def removeFromQ(k: K): (Option[K], LIRSStacks[K]) = {
    val (optK, newStackQ) = stackQ.remove(k)
    (optK, new LIRSStacks(stackS, newStackQ))
  }

  def isOldestInS(k: K): Boolean = stackS.isOldest(k)

  def isOldestInQ(k: K): Boolean = stackQ.isOldest(k)

  def isSFull: Boolean = stackS.isFull

  def isQFull: Boolean = stackQ.isFull

  def isInS(k: K): Boolean = stackS.contains(k)

  def isInQ(k: K): Boolean = stackQ.contains(k)

  def evict(k: K): LIRSStacks[K] = new LIRSStacks(stackS.remove(k)._2, stackQ.remove(k)._2)

  def empty = new LIRSStacks(stackS.empty, stackQ.empty)

  override def toString = "S:["+stackS+"] Q:["+stackQ+"]"
}

object LIRSCache {
  def apply[K, V](maxSize:Int, sPercent:Double, backingMap:Map[K, V] = Map.empty[K,V]) = {
    val sSize = (maxSize * sPercent).toInt
    val qSize = maxSize - sSize
    require(sSize > 0, "Size of S stack in cache must be >0")
    require(qSize > 0, "Size of Q stack in cache must be >0")
    new LIRSCache[K, V](LIRSStacks[K](sSize, qSize), backingMap)
  }
}

/**
 * This is an implementation of an immutable LIRS Cache based on the LIRS Cache impelementation
 * in Clojure's core.cache:
 * https://github.com/clojure/core.cache/blob/master/src/main/clojure/clojure/core/cache.clj.
 * The cache is described in this paper:
 * http://citeseer.ist.psu.edu/viewdoc/download;jsessionid=EA23F554FDF98A258C6FDF0C8E98BFD1?doi=10.1.1.116.2184&rep=rep1&type=pdf
 */

class LIRSCache[K, V](lirsStacks:LIRSStacks[K], backingMap: Map[K, V]) extends Cache[K, V] {
  def get(k: K): Option[V] = backingMap.get(k)

  def put(kv: (K, V)): (Set[K], Cache[K, V]) = {
    val (k, v) = kv
    def miss:(Set[K], Cache[K, V]) = {
      if (!lirsStacks.isSFull) {
        // We know that S is not full, so we should not need to worry about eviction.
        // In this case, S is not full, so we just add our key to S, and put the kv in the cache
        val (older, newLirsStacks) = lirsStacks.putOnTopOfS(k)
        older match {
          case None => (Set.empty[K], new LIRSCache(newLirsStacks, backingMap + kv))
          case _ => throw new IllegalStateException("Stack was not full, yet evicted when element was added")
        }
      } else {
        val (evictedFromS, newLirsStacks1) = lirsStacks.putOnTopOfS(k)
        evictedFromS match {
          case Some(oldK) => {
            // We know that S is full, and that k is not in S. We add our thing to Q, and take out of the cache anything that comes out.
            // We then add it to S, and if we get something out, take it out of the cache if it is also not in Q. We add kv to the cache.
            val (evictedFromQ, newLirsStacks2) = newLirsStacks1.putOnTopOfQ(k)
            val (evicted1, newBackingMap1) = evictedFromQ match {
              case Some(oldK) => (Set(oldK), backingMap - oldK)
              case None => (Set.empty[K], backingMap)
            }
            val (evicted2, newBackingMap2) =
              if (newLirsStacks2.isInQ(oldK)) {
                (evicted1, newBackingMap1)
              } else {
                (evicted1 + oldK, newBackingMap1 - oldK)
              }
              (evicted2, new LIRSCache(newLirsStacks2, newBackingMap2 + kv))
          }
          case None => {
            // We know that S is full, and that k is in S. This is a non-resident HIR block. We have bumped k in S and got nothing
            // back. Then we take the oldest element of S and add it to Q. If we get something back, we take it out of the cache.
            // Then we prune.
            val (oldestInS, newLirsStacks2) = newLirsStacks1.dropOldestInS
            oldestInS match {
              case Some(oldKinS) => {
                val (evictedFromQ, newLirsStacks3) = newLirsStacks2.putOnTopOfQ(oldKinS)
                val (evicted, newBackingMap1) = evictedFromQ match {
                  case Some(oldKinQ) => (Set(oldKinQ), backingMap - oldKinQ)
                  case None => (Set.empty[K], backingMap)
                }
                (evicted, new LIRSCache(newLirsStacks3.prune, newBackingMap1))
              }
              case None => (Set.empty[K], new LIRSCache(newLirsStacks2.prune, backingMap))
            }
          }
        }
      }
    }
    get(k) match {
      case Some(oldV) => if (oldV != v) miss else (Set.empty[K], this)
      case None => miss
    }
  }

  def hit(k: K): Cache[K, V] =
    get(k).map { v =>
      if (lirsStacks.isInS(k) && !lirsStacks.isInQ(k)) {
        // In the case where k is in S but not in Q, it is a LIR block. We push it to the top of S, and
        // prune if it was the oldest in S.
        val (evictedFromS, newLirsStacks) = lirsStacks.putOnTopOfS(k)
        if (evictedFromS.isDefined) {
          throw new IllegalStateException("Nothing should have been evicted from S when k was bumped as it was already present")
        }
        new LIRSCache(if (lirsStacks.isOldestInS(k)) newLirsStacks.prune else newLirsStacks, backingMap)
      } else if (lirsStacks.isInS(k) && lirsStacks.isInQ(k)) {
        // In the case where k is in S and Q, it is an HIR block. We bump k to the top of S and remove it from Q.
        // We then move the oldest value in S to Q. Then we prune.
        val (evictedFromS, newLirsStacks) = lirsStacks.putOnTopOfS(k)
        if (evictedFromS.isDefined) {
          throw new IllegalStateException("Nothing should have been evicted from S when k was bumped as it was already present")
        }
        val (evictedFromQ, newLirsStacks2) = newLirsStacks.removeFromQ(k)
        if (!evictedFromQ.isDefined) {
          throw new IllegalStateException("Key was not evicted from Q despite it being present")
        }
        val (oldestInS, newLirsStacks3) = newLirsStacks2.dropOldestInS
        oldestInS match {
          case Some(oldK) => {
            val (evictedFromQ2, newLirsStacks4) = newLirsStacks3.putOnTopOfQ(oldK)
            if (evictedFromQ2.isDefined) {
              throw new IllegalStateException("Nothing should have been evicted when we put the value from S on top of Q")
            }
            new LIRSCache(newLirsStacks4.prune, backingMap)
          }
          case None => throw new IllegalStateException("We dropped the oldest value in S but got nothing back")
        }
      } else if (!lirsStacks.isInS(k) && lirsStacks.isInQ(k)) {
        // In the case where k is not in S but it is in Q it is a non-resident HIR block. We bump it to the top of Q, and put it in 
        // S. If anything is evicted from S in the process, we check if it is in Q. If it is not, we remove it from the cache.
        val (evictedFromQ, newLirsStacks) = lirsStacks.putOnTopOfQ(k)
        if (evictedFromQ.isDefined) {
          throw new IllegalStateException("We bumped the value to the top of Q, so nothing should have come back")
        }
        val (evictedFromS, newLirsStacks2) = newLirsStacks.putOnTopOfS(k)
        val newBackingMap = evictedFromS match {
          case Some(oldK) if !newLirsStacks2.isInQ(k) => backingMap - oldK
          case _ => backingMap
        }
        new LIRSCache(newLirsStacks2, newBackingMap)
      } else {
        throw new IllegalStateException("Key in cache, but not in Stack S or Stack Q. Key: " + k)
      }
    }.getOrElse(this)
 
  def evict(k: K): (Option[V], Cache[K, V]) =
    (get(k), new LIRSCache(lirsStacks.evict(k), backingMap - k))

  def iterator: Iterator[(K, V)] = backingMap.iterator

  def empty: Cache[K, V] = new LIRSCache(lirsStacks.empty, backingMap.empty)

  override def toString = {
    val pairStrings = iterator.map { case (k, v) => k + " -> " + v }
    "LIRSCache(" + pairStrings.toList.mkString(", ") + ")"
  }
}