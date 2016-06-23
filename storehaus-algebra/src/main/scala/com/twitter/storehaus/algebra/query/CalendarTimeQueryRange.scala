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

import com.twitter.scalding.{RichDate, DateRange, Duration, AbsoluteDuration, Years,
  Months, Days, Hours, Minutes}
import java.util.TimeZone
import scala.annotation.tailrec

case class CalendarBucket(typeIndx: Int, startTime: Long)

/** A query strategy for time with named buckets.
 */
class CalendarTimeStrategy(strategyTimezone: TimeZone = TimeZone.getTimeZone("UTC"))
    extends QueryStrategy[DateRange, Long, CalendarBucket] {

  private val allBuckets = {
    implicit val tz = strategyTimezone
    List(Years(1), Months(1), Days(1), Hours(1), Minutes(1))
  }

  def bucketLength(bucket: CalendarBucket): Long = {
    val endTime = indexToDuration(bucket.typeIndx).addTo(RichDate(bucket.startTime)).timestamp
    endTime - bucket.startTime
  }

  protected def indexToDuration(indx: Int) = allBuckets(indx)
  protected def durationToName(x: Duration): String = x.getClass.getName.split('.').last

  private def tsToBuckets(msSinceEpoch: Long) = {
    val richDate = RichDate(msSinceEpoch)
    allBuckets.zipWithIndex.map { case (duration, indx) =>
      CalendarBucket(indx, duration.floorOf(richDate).timestamp)
    }.toSet
  }

  private def len(dr: DateRange) =
    AbsoluteDuration.fromMillisecs(dr.end.timestamp - dr.start.timestamp + 1L)

  private def outsideRange(filterDR: DateRange, child: DateRange) =
    child.start >= filterDR.end || child.end <= filterDR.start

  def query(dr: DateRange): Set[CalendarBucket] =
    extract(dr, Set(dr), 0, allBuckets, Set[CalendarBucket]())

  @tailrec
  private def extract(
    filterDr: DateRange,
    drSet: Set[DateRange],
    curIndx: Int,
    remainingDurations: List[Duration],
    acc: Set[CalendarBucket]
  ): Set[CalendarBucket] = {
    remainingDurations match {
      case Nil => acc
      case head :: tail =>
        // expand the DR
        val expandedOut = drSet.map { dr =>
          DateRange(head.floorOf(dr.start), head.floorOf(dr.end) + head)
            .each(head)
            .filter(!outsideRange(filterDr, _))
            .filter(len(_).toMillisecs > 1L)
            .toSet
        }.foldLeft(Set[DateRange]())(_ ++ _)
        // Things which only partially fit in this time range
        val others = expandedOut.filter(!filterDr.contains(_))
        // Things which fit fully in this time range
        val fullyInRange = expandedOut
          .filter(filterDr.contains)
          .map(x => CalendarBucket(curIndx, x.start.timestamp))
        extract(filterDr, others, curIndx + 1, tail, acc ++ fullyInRange)
    }
  }

  def index(ts: Long): Set[CalendarBucket] = tsToBuckets(ts)
}
