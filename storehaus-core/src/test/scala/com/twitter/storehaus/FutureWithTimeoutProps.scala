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

package com.twitter.storehaus

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose
import org.scalacheck.Prop._

import com.twitter.util.{Future, Return, Duration}
import java.util.concurrent.TimeUnit

object FutureWithDefaultProperties extends Properties("FutureWithDefault") {

  implicit def arbFut[T](implicit arb: Arbitrary[T]): Arbitrary[FutureWithDefault[T]] = {
    Arbitrary {
      for(t0 <- arb.arbitrary; t1 <- arb.arbitrary)
        yield FutureWithDefault.onTimeout(Future.value(t0))(Return(t1))
    }
  }
  property("associativity") = forAll { (fm: FutureWithDefault[Int]) =>
    val fn1 = { x: Int => FutureWithDefault.value(10*x) }
    val fn2 = { x: Int => FutureWithDefault.value(42*x) }
    fm.flatMap(fn1).flatMap(fn2).getNow ==
      fm.flatMap { fn1.andThen { _.flatMap(fn2) } }.getNow
  }

  property("map is consistent with flatMap") = forAll { (fm: FutureWithDefault[Int]) =>
    fm.map { _ * 10 }.getNow == fm.flatMap { i => FutureWithDefault.value(10*i) }.getNow
  }

  property("Constants are returned") = forAll { (i: Int) =>
    FutureWithDefault.value(i).getNow == Return(i) &&
      FutureWithDefault.value(i).get(Duration(1, TimeUnit.SECONDS)) == Return(i)
  }
}
