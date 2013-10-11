package com.twitter.storehaus.cache

sealed trait Side extends Ordered[Side] {
  def nextSide:Side
  override def toString = getClass.getSimpleName
  def compare(that:Side) =
    if (this == that) {
      0
    } else if (nextSide == that) {
      -1
    } else {
      1
    }
}
object SideA extends Side {
  def nextSide = SideB
}
object SideB extends Side {
  def nextSide = SideC
}
object SideC extends Side {
  def nextSide = SideA
}

object CyclicIncrementProvider {
  def intIncrementer: CyclicIncrementProvider[Int] =
    CyclicIncrementProvider[Int](0, {i:Int => i + 1}, SideA, 0, 0, 0, 0)

  def apply[K:Ordering](zero: K, increment: K => K): CyclicIncrementProvider[K] =
    CyclicIncrementProvider(zero, increment, SideA, 0, zero, 0, zero)

  def apply[K:Ordering](zero: K,
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
class CyclicIncrementProvider[K:Ordering]
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
//TODO(jcoveney) in the companion object give it an implicit ordering by the Tuple of values, then havet he Side ordering in scope
case class CyclicIncrement[K:Ordering](side:Side, value:K) {
  override def toString = side + ":" + value
}