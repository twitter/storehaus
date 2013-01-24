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

import com.twitter.util.{ Future => TFuture }
import scala.actors.{ Actor, TIMEOUT }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * The StoreSynchronizer allows the user to snapshot all key-value pairs
 * from an immutable KeySetStore into some different MutableStore
 * at a specified interval. For efficiency, the StoreSynchronizer only sends
 * key-value pairs that have changed since its last synchronization.
 *
 * StoreSynchronizer instances close over a millisecond interval and a mutable
 * backing store and accept messages containing a reference to an immutable
 * KeySetStore. A store using a StoreSynchronizer should send a new KeySetStore
 * reference to the StoreSynchronizer on every write.
 */

class StoreSynchronizer[K,V](interval: Long, toStore: MutableStore[_ <: MutableStore[_,K,V],K,V]) extends Actor {
  def act {
    var stop = false
    var nextTime = nowPlusInterval
    var prev: KeysetStore[_,K,V] = new MapStore[K,V]()
    var next: KeysetStore[_,K,V] = prev
    loopWhile (!stop) {
      // Since processing can take a non-negligible amount of time,
      // nextInterval(nextTime)'s used to guarantee some sort of
      // regularity on the interval.
      //
      // TODO: Log a warning message if nextInterval drops to zero.
      reactWithin(nextInterval(nextTime)) {
        // If the store sends the stop keyword, process once more and kill.
        case 'stop => {
          transfer(next, prev).apply
          toStore.close
          prev = next
          stop = true
        }

        // This case is reached approximately once per interval and
        // triggers the transfer from the immutable store's state at
        // this instant over to the mutable backing store.
        case TIMEOUT => {
          nextTime = nowPlusInterval
          transfer(next, prev).apply
          prev = next
        }

        // Swap the immutable backing store reference at every key
        // increment.
        case kv: KeysetStore[_,K,V] => {
          next = kv
        }
      }
    }
  }
  def nowPlusInterval : Long = interval + System.currentTimeMillis

  def nextInterval(nextTime : Long) : Long = 0L max (nextTime - System.currentTimeMillis)

  private def cast[T <: MutableStore[T,K,V]](obj: Any): MutableStore[T,K,V] = obj.asInstanceOf[MutableStore[T,K,V]]

  protected def processStoreFuture[MS <: MutableStore[MS,K,V]](f: TFuture[Seq[(K,Option[V])]], mutableStore: MutableStore[MS,K,V])
  : TFuture[MutableStore[MS,K,V]] = {
    f.flatMap { pairs: Seq[(K,Option[V])] =>
      pairs
        .foldLeft(TFuture.value(mutableStore)) { (storeFuture, pair) =>
          val (k, optV) = pair
          optV map { v =>
            storeFuture flatMap { _ + (k -> v) }
          } getOrElse {
            storeFuture flatMap { _ - k }
          } map { cast[MS](_) }
        }
    }
  }

  // TODO: the memcache could throw. Do some more work into catching throwables here.
  protected def transfer[MS <: MutableStore[MS,K,V]](newValues: KeysetStore[_,K,V],
                                                     prevStore: KeysetStore[_,K,V]) = {
    val newKeySet = newValues.keySet
    val futurePairs: TFuture[Seq[(K,Option[V])]] = {
      val futureSeq = newKeySet.toSeq
        .map { k =>
          newValues.get(k)
            .join(prevStore.get(k))
            .map { case (newV, oldV) =>
              if (newV != oldV)
                Some((k -> newV))
              else
                None
            }
        }
      TFuture.collect(futureSeq) map { _.flatten }
    }

    val futureStore = processStoreFuture[MS](futurePairs, cast[MS](toStore))

    // Remove the keys that are now absent
    (prevStore.keySet -- newKeySet)
      .foldLeft(futureStore) { (s, k) => s flatMap { _ - k } map { cast[MS](_) } }
      .flatMap { _ => TFuture.Void }
  }
}
