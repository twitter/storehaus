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

/**
 * This is an immutable implementation of a provider of cyclical increments. The motivation
 * is that we need ordered identifiers for caches, but we do not want to just increment
 * an int as this has a theoretical upper limit (this is functional programming, our stuff
 * doesn't crash, right?). This class allows the reuse of numbers, making it highly unlikely
 * to run out of identifiers (it is still possible, but in a way that would be unavoidable -- 
 * if there is a key that stays in the stacks forever, thus never allowing us to reuse the
 * increments after that key). This class ensures that when it is asked for a new increment,
 * it will be greater than all currently outstanding increments at that time.
 *
 * @author Jonathan Coveney
 */

object CyclicIncrementProvider {
  def intIncrementer: CyclicIncrementProvider[Int] = CyclicIncrementProvider(0, {i:Int => i + 1})

  def apply[@specialized(Int) K:Ordering](zero: K, increment: K => K): CyclicIncrementProvider[K] =
    CyclicIncrementProvider(zero, increment, SideA, 0, zero, 0, zero)

  def apply[@specialized(Int) K:Ordering](zero: K,
               increment: K => K,
               currentSide: Side,
               currentSideCount: Int,
               maxCurrentSideVal: K,
               nextSideCount: Int,
               maxNextSideVal: K): CyclicIncrementProvider[K] = 
    new CyclicIncrementProvider[K](zero, increment, currentSide, currentSideCount, maxCurrentSideVal, nextSideCount, maxNextSideVal)
}

// Algorithm: we start on a side. We hand out values. Once we've handed out at least 1 value, we begin to give out values of side.nextSide. Once
// all of the increments of the previous side have been culled, we now switch. side.nextSide becomes the current side. Then we repeat the algorithm.
class CyclicIncrementProvider[@specialized(Int) K:Ordering]
  (zero: K,
   increment: K => K,
   currentSide: Side,
   currentSideCount: Int,
   maxCurrentSideVal: K,
   nextSideCount: Int,
   maxNextSideVal: K) {
  def getNewMaxIncrement:(CyclicIncrement[K], CyclicIncrementProvider[K]) =
    if (nextSideCount > 0 || currentSideCount > 0) {
      // We hand one out of the next time
      val nextVal = increment(maxNextSideVal)
      (CyclicIncrement[K](currentSide.nextSide, nextVal),
       CyclicIncrementProvider[K](zero, increment, currentSide, currentSideCount, maxCurrentSideVal, nextSideCount+1, nextVal))
    } else {
      // We hand out one of the current time
      val nextVal = increment(maxCurrentSideVal)
      (CyclicIncrement[K](currentSide, nextVal),
       CyclicIncrementProvider[K](zero, increment, currentSide, currentSideCount+1, nextVal, nextSideCount, maxNextSideVal))
    }

  def cullOldIncrement(cyclicIncrement:CyclicIncrement[K]):CyclicIncrementProvider[K] =
    if (cyclicIncrement.side == currentSide) {
      val nextCurrentSidecount = currentSideCount - 1
      if (nextCurrentSidecount == 0) {
        CyclicIncrementProvider[K](zero, increment, currentSide.nextSide, nextSideCount, maxNextSideVal, 0, zero)
      } else {
        CyclicIncrementProvider[K](zero, increment, currentSide, nextCurrentSidecount, maxCurrentSideVal, nextSideCount, maxNextSideVal)
      }
    } else if (cyclicIncrement.side == currentSide.nextSide) {
      CyclicIncrementProvider[K](zero, increment, currentSide, currentSideCount, maxCurrentSideVal, nextSideCount-1, maxNextSideVal)
    } else {
      throw new IllegalStateException("Shouldn't be culling a value of given type")
    }

    override def toString =
      "CyclicIncrementProvider: zero:%d currentSide:%s currentSideCount:%d maxCurrentSideVal:%d nextSideCount:%d maxNextSideVal:%d"
        .format(zero, currentSide, currentSideCount, maxCurrentSideVal, nextSideCount, maxNextSideVal)

    def empty = CyclicIncrementProvider(zero, increment)
}

object CyclicIncrement {
  implicit def ordering[K](implicit ordering:Ordering[K]):Ordering[CyclicIncrement[K]] = Ordering.by { ci => (ci.side, ci.value) }
}

case class CyclicIncrement[@specialized(Int) K:Ordering](side:Side, value:K) {
  override def toString = side + ":" + value
}

sealed trait Side extends Ordered[Side] {
  val nextSide:Side
  override def toString = getClass.getSimpleName
  def compare(that:Side) = if (this == that) 0 else if (nextSide == that) -1 else 1
}
object SideA extends Side { val nextSide = SideB }
object SideB extends Side { val nextSide = SideC }
object SideC extends Side { val nextSide = SideA }