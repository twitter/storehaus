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

package com.twitter.storehaus.algebra.query


import org.scalacheck.{ Gen, Arbitrary, Properties }
import org.scalacheck.Prop._
import org.scalacheck.Properties
import com.twitter.scalding._
import scala.collection.breakOut

object CalendarTimeQueryRangeProperties extends Properties("CalendarTimeStrategy") {
  def exclusiveUpperLen(dr: DateRange) = AbsoluteDuration.fromMillisecs(dr.end.timestamp - dr.start.timestamp )

  def floor(ts:Long, floorSize: Long = 1000L * 60) = (ts / floorSize) * floorSize

  val TOP_DATE_SPAN = 1000L * 3600 * 24 * 365 * 80
  implicit val arbDR:Arbitrary[DateRange] = Arbitrary(for {
      start <- Gen.choose(0, TOP_DATE_SPAN)
      endDelta <- Gen.choose(0, TOP_DATE_SPAN/2)
    } yield {
      val startTS = floor(start)
      val endTS = startTS + floor(endDelta)
      DateRange(RichDate(startTS), RichDate(endTS))
      })

  property("Index timerange must have an entry in the query") = forAll { dr: DateRange =>
    val strategy = new CalendarTimeStrategy
    val buckets = strategy.query(dr)
    val queryBuckets = strategy.index(dr.start.timestamp)
    (queryBuckets & buckets).size == 1
  }

  property("Time QueryRange Adds back up tot the expected total") = forAll { dr: DateRange =>
    val strategy = new CalendarTimeStrategy
    val timeLen = exclusiveUpperLen(dr).toMillisecs
    val buckets = strategy.query(dr)
    val reSummed = buckets.toList.map(_.length).foldLeft(0L){_ + _}
    reSummed == timeLen
  }
}
