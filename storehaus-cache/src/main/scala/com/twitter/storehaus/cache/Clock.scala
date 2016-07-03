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
 * A clock is anything which provides a succession of values.
 */

trait Clock[@specialized(Int, Long) T, +This <: Clock[T, This]] {
  def tick: (T, This)
  def empty: This
}

trait IdProvider[@specialized(Int, Long) T] extends Clock[T, IdProvider[T]] {
  def cull(oldId: T): IdProvider[T]
}

case class LongClock(v: Long = 0) extends Clock[Long, LongClock] {
  def tick: (Long, LongClock) = (v, LongClock(v + 1))
  def empty: LongClock = LongClock()
}
