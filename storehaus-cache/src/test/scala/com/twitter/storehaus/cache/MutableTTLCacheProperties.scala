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

object MutableTTLCacheProperties extends Properties("MutableTTLCache") {
  case class TTLTime(get: Long)
  case class DeltaTime(get: Long)
  implicit def genTTLTime: Arbitrary[TTLTime] = Arbitrary (
      for {
        x <- choose(20, 40)
      } yield TTLTime(x)
    )
  implicit def genDeltaTime: Arbitrary[DeltaTime] = Arbitrary (
      for {
        x <- choose(10, 20)
      } yield DeltaTime(x)
    )


  property("If insert an item with a TTL of X, and wait X + delta then it cannot be there") = forAll { (ttl: TTLTime, delta: DeltaTime, items: List[Long])  =>
      val cache = MutableCache.ttl[Long, Long](ttl.get, items.size + 2)
      items.foreach{ item =>
        cache += (item, item)
      }
      Thread.sleep(ttl.get + delta.get)
      items.forall{ item =>
        cache.get(item) == None
      }
  }

  property("if you put with TTL t, and wait 0 time, it should always (almost, except due to scheduling) be in there") = forAll { (ttl: TTLTime, delta: DeltaTime, items: List[Long])  =>
      val cache = MutableCache.ttl[Long, Long](ttl.get, items.size + 2)
      items.foreach{ item =>
        cache += (item, item)
      }
      Thread.sleep(ttl.get - delta.get)
      items.forall{ item =>
        cache.get(item) == Some(item)
      }
  }
// I think you can make a law here: if you put something with a TTL of T, and wait >= T, then it cannot be in there.
//  Also, if you put with TTL t, and wait 0 time, it should always (almost, except due to scheduling) be in there.
}
