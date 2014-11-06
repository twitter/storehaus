package com.twitter.storehaus

import com.twitter.util.Await
import org.specs2.mutable.Specification

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

      val secondlyStores = Set(
        (mappingFn(_: Long, 0, 15), secondlyStore1),
        (mappingFn(_: Long, 15, 30), secondlyStore2),
        (mappingFn(_: Long, 30, 45), secondlyStore3),
        (mappingFn(_: Long, 45, 60), secondlyStore4))

      //fanout function that takes a minute and fan it out to seconds
      val fanout = (minute: Long) => (0L until (minute * 60)).toSet

      //create a minutly store by wrapping all the secondly stores
      val minutlyStore = FanoutStore[Long, Long, ReadableStore[Long, Long]](fanout, secondlyStores)

      val result = Await.result(minutlyStore.get(1))
      result.get must_== 60
    }
  }

  def mockSecondlyStore(start: Long, end: Long): ReadableStore[Long, Long] = {
    ReadableStore.fromMap((start to end).map(i => i -> 1L).toMap)
  }

  def mappingFn(x: Long, start: Long, end: Long) = x >= start && x <= end
}
