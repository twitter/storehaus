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

object ClockProperties extends Properties("Clock") {
  //TODO is there a way to make this also work for IdProviders through generics?
  def vectorOfTicks[T: Ordering, K <: Clock[T, K]](clock: K, initialTicks: Vector[T] = Vector.empty[T], number: Int = 100): (Seq[T], K) =
    1.to(number).foldLeft((initialTicks, clock)) { case ((cum, clock), _) =>
      val (v, newClock) = clock.tick
      (cum :+ v, newClock)
    }

  def alwaysDistinct[T: Ordering, K <: Clock[T, K]](clock: K) = {
    val (vector, _) = vectorOfTicks[T, K](clock)
    vector.size == vector.toSet.size
  }

  def alwaysIncreasing[T: Ordering, K <: Clock[T, K]](clock: K) = {
    val (vector, _) = vectorOfTicks[T, K](clock)
    vector == vector.sorted
  }

  def clockLaws[T: Ordering, K <: Clock[T, K]](clock: K) =
    alwaysDistinct[T, K](clock) && alwaysIncreasing[T, K](clock)

  property("LongClock obeys the Clock laws") = clockLaws[Long, LongClock](LongClock())
}

object IdProviderProperties extends Properties("IdProvider") {
  def alwaysIncreasing[T: Ordering](idProvider: IdProvider[T]) = {
    val r = new java.util.Random
    def iter(vector: Vector[T], idProvider: IdProvider[T]) = {
      val (vector, newIdProvider) = ClockProperties.vectorOfTicks[T, IdProvider[T]](idProvider)
      val shouldCull = vector map { _ => r.nextBoolean }
      vector.zip(shouldCull).foldLeft((Vector.empty[T], newIdProvider)) { case ((vector, idProvider), (v, shouldCull)) =>
        if (shouldCull) (vector, idProvider.cull(v)) else (vector :+ v, idProvider)
      }
    }
    val (vector, newIdProvider) = iter(Vector.empty[T], idProvider)
    1.to(100).foldLeft((Vector.empty[T], idProvider, true)) { case ((vector, idProvider, isWorking), _) =>
      val (newVector, newIdProvider) = iter(vector, idProvider)
      (newVector, newIdProvider, isWorking || (vector == vector.sorted && vector.size == vector.toSet.size))
    }._3
  }


  def idProviderLaws[T: Ordering](idProvider: IdProvider[T]) =
    ClockProperties.clockLaws[T, IdProvider[T]](idProvider) && alwaysIncreasing[T](idProvider)

  //TODO(jco) have a test which culls value and such and ensures that global order is always maintained.
  // ie that the new value is always bigger than all the old ones.

  property("CyclicIncrementProvider obeys the IdProvider laws") =
    idProviderLaws(CyclicIncrementProvider.intIncrementer)
}
