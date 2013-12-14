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

import com.twitter.scalding.{RichDate, DateRange, Duration, AbsoluteDuration, Years, Months, Weeks, Days, Hours, Minutes}
import java.util.TimeZone
import scala.annotation.tailrec

case class Bucket(typeIndx: Int, startTime: Long) {
  override def toString = Buckets.durationToName(Buckets.indexToDuration(typeIndx)) + " starting at : " + RichDate(startTime) + "Indx is : " + typeIndx
  def length: Long = {
    val endTime = Buckets.indexToDuration(typeIndx).addTo(RichDate(startTime)).timestamp
    endTime - startTime
  }
}
object Buckets {
  private val allBuckets = {
    implicit val tz = TimeZone.getTimeZone("UTC")
    List(Years(1), Months(1), Days(1), Hours(1), Minutes(1))
  }

  def indexToDuration(indx: Int) = allBuckets(indx)

  def durationToName(x: Duration): String = x.getClass.getName.split('.').reverse.head

  def get = allBuckets
  // def getDuration(bucket: Bucket) = allBuckets(bucket.indx)
  // def getName(bucket: Bucket) = durationToName(allBuckets(bucket.indx))
  def tsToBuckets(msSinceEpoch: Long) =  {
    val richDate = RichDate(msSinceEpoch)
    allBuckets.zipWithIndex.map { case (duration, indx) =>
      Bucket(indx, duration.floorOf(richDate).timestamp)
    }.toSet
  }
}

/** A query strategy for time with named buckets.
 */
class CalendarTimeStrategy extends QueryStrategy[DateRange, Long, Bucket] {
  private def len(dr: DateRange) = AbsoluteDuration.fromMillisecs(dr.end.timestamp - dr.start.timestamp + 1L)

  private def outsideRange(filterDR: DateRange, child: DateRange) =
    (child.start >= filterDR.end || child.end <= filterDR.start)

  def query(dr: DateRange): Set[Bucket] = extract(dr, Set(dr), 0, Buckets.get, Set[Bucket]())

  @tailrec
  private def extract(filterDr: DateRange, drSet: Set[DateRange], curIndx: Int, remainingDurations: List[Duration], acc: Set[Bucket]): Set[Bucket] = {
    remainingDurations match {
      case Nil => acc
      case head :: tail =>
      // expand the DR
        val expandedOut = drSet.map{ dr =>
                            DateRange(head.floorOf(dr.start), head.floorOf(dr.end) + head)
                                .each(head)
                                .filter(!outsideRange(filterDr, _))
                                .filter(len(_).toMillisecs > 1L)
                                .toSet
                            }.foldLeft(Set[DateRange]()){_ ++ _}
        // Things which only partially fit in this time range
        val others = expandedOut.filter(!filterDr.contains(_))
        // Things which fit fully in this time range
        val fullyInRange = expandedOut
                              .filter(filterDr.contains(_))
                              .map(x => Bucket(curIndx, x.start.timestamp))
        extract(filterDr, others, curIndx + 1, tail, acc ++ fullyInRange)
    }
  }

  def index(ts: Long) = Buckets.tsToBuckets(ts)
}
