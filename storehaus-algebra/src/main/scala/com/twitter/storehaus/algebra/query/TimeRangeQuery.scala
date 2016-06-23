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

import scala.annotation.tailrec

// end is not included: [start, end)
case class TimeRange(start: Long, end: Long)
/*
 *  bucket is which of the exponentially size buckets we are in
 * timeInBuckets = time / (this bucket size)
 */
case class BucketTime(bucket: Int, timeInBuckets: Long)

/** A query strategy for time with exponentially expanding buckets of time.
 */
class TimestampStrategy(minBucketMs: Long,
  maxBucketMs: Long, factor: Int) extends QueryStrategy[TimeRange, Long, BucketTime] {

  val buckets = Iterator.iterate(minBucketMs) { _ * factor }
    .takeWhile { _ < maxBucketMs}
    .zipWithIndex
    .toList

  // Maps start/end to the the greatest lower bound
  def normalize(in: TimeRange): TimeRange =
    TimeRange(toBoundary(in.start), toBoundary(in.end))

  protected def toBoundary(time: Long): Long =
    (time / minBucketMs) * minBucketMs

  def query(thisquery: TimeRange): Set[BucketTime] = {
    // find the minimal covering set of [start,end]
    @tailrec
    def queryRec(q: TimeRange, acc: Set[BucketTime]): Set[BucketTime] = {
      if (q.start >= q.end) acc
      else {
        val (bucketSize, idx) = buckets.dropWhile { case (size, _) =>
          (q.start % size == 0) && (q.start + size <= q.end)
        }.head
        queryRec(TimeRange(q.start + bucketSize, q.end),
          acc + BucketTime(idx, q.start/bucketSize))
      }
    }
    // Kick off the recursion
    queryRec(thisquery, Set[BucketTime]())
  }

  def index(ts: Long): Set[BucketTime] =
    buckets.map { case (bucketSize, idx) => BucketTime(idx, ts/bucketSize) }.toSet
}
