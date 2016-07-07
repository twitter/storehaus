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

import org.scalacheck.{Prop, Properties}
import org.scalacheck.Prop._

object ClockProperties extends Properties("Clock") {
  def vectorOfTicks[T: Ordering, K <: Clock[T, K]](
      clock: K, initialTicks: Vector[T] = Vector.empty[T], number: Int = 100): (Seq[T], K) =
    1.to(number).foldLeft((initialTicks, clock)) { case ((cum, c), _) =>
      val (v, newClock) = c.tick
      (cum :+ v, newClock)
    }

  def alwaysDistinct[T: Ordering, K <: Clock[T, K]](clock: K): Boolean = {
    val (vector, _) = vectorOfTicks[T, K](clock)
    vector.size == vector.toSet.size
  }

  def alwaysIncreasing[T: Ordering, K <: Clock[T, K]](clock: K): Boolean = {
    val (vector, _) = vectorOfTicks[T, K](clock)
    vector == vector.sorted
  }

  def clockLaws[T: Ordering, K <: Clock[T, K]](clock: K): Boolean =
    alwaysDistinct[T, K](clock) && alwaysIncreasing[T, K](clock)

  property("LongClock obeys the Clock laws") = clockLaws[Long, LongClock](LongClock())
}

object IdProviderProperties extends Properties("IdProvider") {
  def alwaysIncreasing[T: Ordering](idProvider: IdProvider[T]): Boolean = {
    val r = new java.util.Random
    def iter(vector: Vector[T], idProvider: IdProvider[T]) = {
      val (vector, newIdProvider) = ClockProperties.vectorOfTicks[T, IdProvider[T]](idProvider)
      val shouldCull = vector map { _ => r.nextBoolean }
      vector.zip(shouldCull)
        .foldLeft((Vector.empty[T], newIdProvider)) { case ((vec, idP), (v, shouldC)) =>
          if (shouldC) (vec, idP.cull(v)) else (vec :+ v, idP)
        }
    }
    (1 to 100).foldLeft((Vector.empty[T], idProvider, true)) { case ((vec, idP, isWorking), _) =>
      val (newVector, newIdProvider) = iter(vec, idP)
      (newVector, newIdProvider, isWorking || (vec == vec.sorted && vec.size == vec.toSet.size))
    }._3
  }

  def idProviderLaws[T: Ordering](idProvider: IdProvider[T]): Prop =
    ClockProperties.clockLaws[T, IdProvider[T]](idProvider) && alwaysIncreasing[T](idProvider)

  property("CyclicIncrementProvider obeys the IdProvider laws") =
    idProviderLaws(CyclicIncrementProvider.intIncrementer)
}
