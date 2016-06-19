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

import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._
import com.twitter.conversions.time._

object MutableTTLCacheProperties extends Properties("MutableTTLCache") {
  case class PositiveTTLTime(get: Long)
  case class NegativeTTLTime(get: Long)
  implicit def genPositiveTTLTime: Arbitrary[PositiveTTLTime] = Arbitrary (
      for {
        x <- choose(0, 40)
      } yield PositiveTTLTime(x)
    )
   implicit def genNegativeTTLTime: Arbitrary[NegativeTTLTime] = Arbitrary (
      for {
        x <- choose(80, 480)
      } yield NegativeTTLTime(x)
    )

  def spinSleep(howLong: Long) {
    val finishTime = System.currentTimeMillis + howLong
    while (System.currentTimeMillis < finishTime) {}
  }

  property("If insert an item with a TTL of X, and wait X + delta then it cannot be there") =
    forAll { (ttl: PositiveTTLTime, items: List[Long])  =>
      val cache = MutableCache.ttl[Long, Long](ttl.get.milliseconds, items.size)
      items.foreach(item => cache += (item, item))
      spinSleep(ttl.get + 40)
      items.iterator.forall(item => cache.get(item).isEmpty)
    }

  property("if you put with TTL t, and wait 0 time, it should always " +
      "(almost, except due to scheduling) be in there") =
    forAll { (ttl: NegativeTTLTime, items: List[Long])  =>
      val cache = MutableCache.ttl[Long, Long](ttl.get.milliseconds, items.size)
      items.foreach(item => cache += (item, item))
      items.forall(item => cache.get(item).isDefined)
    }
}
