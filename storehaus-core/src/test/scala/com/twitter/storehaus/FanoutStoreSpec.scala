package com.twitter.storehaus

import com.twitter.algebird.Monoid
import com.twitter.util.{Future, Await}
import org.scalacheck.{Gen, Arbitrary, Properties}
import org.specs2.mutable.Specification
import org.scalacheck.Gen._
import org.scalacheck.Prop._

/**
 * @author Mansur Ashraf
 * @since 11/6/14
 */
class FanoutStoreSpec extends Specification {
  "Fanout store" should {
    "fan out keys to underlying stores" in {

      /*
        Four mock secondly Stores covering 15 seconds each to make a full minute
       */
      val secondlyStore1 = mockSecondlyStore(0, 15)
      val secondlyStore2 = mockSecondlyStore(15, 30)
      val secondlyStore3 = mockSecondlyStore(30, 45)
      val secondlyStore4 = mockSecondlyStore(45, 60)


      //fanout function that takes a minute and fan it out to seconds
      val fanout = (minute: Long) => (0L until (minute * 60)).toSet

      val storeLookupFn = (i: Long) => i match {
        case x if x >= 0 && x < 15 => secondlyStore1
        case x if x >= 15 && x < 30 => secondlyStore2
        case x if x >= 30 && x < 45 => secondlyStore3
        case x if x >= 45 && x < 60 => secondlyStore4
      }

      //create a minutly store by wrapping all the secondly stores
      val minutlyStore = FanoutStore[Long, Long, Long, ReadableStore[Long, Long]](fanout)(storeLookupFn)

      val result = Await.result(minutlyStore.get(1))
      result.get must_== 60
    }
  }

  def mockSecondlyStore(start: Long, end: Long): ReadableStore[Long, Long] = {
    ReadableStore.fromMap((start until end).map(i => i -> 1L).toMap)
  }

  def mappingFn(x: Long, start: Long, end: Long) = x >= start && x <= end
}

object FanoutStoreProperties extends Properties("FanoutStoreProperties") {

  val stores = Arbitrary {
    for {
      store1 <- containerOf[Set, (Long)](choose(0L, 24L))
      store2 <- containerOf[Set, (Long)](choose(25L, 49L))
      store3 <- containerOf[Set, (Long)](choose(50L, 74L))
      store4 <- containerOf[Set, (Long)](choose(75L, 99L))
    } yield (store1, store2, store3, store4)
  }

  property("fanout store should match scala collection") = {
    forAll(stores.arbitrary) {
      case (s1, s2, s3, s4) =>

        val store1 = ReadableStore.fromMap[Long, Long](s1.map(s => (s, s)).toMap)
        val store2 = ReadableStore.fromMap[Long, Long](s2.map(s => (s, s)).toMap)
        val store3 = ReadableStore.fromMap[Long, Long](s3.map(s => (s, s)).toMap)
        val store4 = ReadableStore.fromMap[Long, Long](s4.map(s => (s, s)).toMap)

        val storeLookupFn = (i: Long) => i match {
          case x if x < 25L => store1
          case x if x >= 25L && x < 50L => store2
          case x if x >= 50L && x < 75L => store3
          case x if x >= 75L => store4
        }

        val fanout = (k: Long) => 0L until k
        val fanoutStore = FanoutStore[Long, Long, Long, ReadableStore[Long, Long]](fanout)(storeLookupFn)

        val expected = (s1 ++ s2 ++ s3 ++ s4).sum

        //Storehaus returns None when no values are found
        (if (expected == 0) None else Some(expected)) =? Await.result(fanoutStore.get(100))
    }
  }


}
