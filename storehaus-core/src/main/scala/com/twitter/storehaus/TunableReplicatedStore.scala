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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.twitter.util.{ Future, Promise, Return, Throw, Time }

/**
 * N - total replicas
 * One - 1 successful operation is sufficient to consider the operation as complete
 * Quorum - N/2 + 1 successful operations are sufficient to consider the operation as complete
 * All - N successful operations are required
 */
sealed trait ConsistencyLevel {
  def expectedSuccesses[K, V](stores: Seq[ReadableStore[K, V]]): Int
}

object ConsistencyLevel {
  case object One extends ConsistencyLevel {
    override def expectedSuccesses[K, V](stores: Seq[ReadableStore[K, V]]): Int = 1
  }
  case object Quorum extends ConsistencyLevel {
    override def expectedSuccesses[K, V](stores: Seq[ReadableStore[K, V]]): Int =
      stores.size / 2 + 1
  }
  case object All extends ConsistencyLevel {
    override def expectedSuccesses[K, V](stores: Seq[ReadableStore[K, V]]): Int = stores.size
  }
}

/**
 * Thrown when a read operation fails due to unsatisfied consistency level
 */
class ReadFailedException[K](val key: K)
    extends RuntimeException("Read failed for key " + key)

/**
 * Thrown when a write operation fails due to unsatisfied consistency level
 */
class WriteFailedException[K](val key: K)
    extends RuntimeException("Write failed for key " + key)

/**
 * Replicates reads to a seq of stores and returns the value based on picked read consistency.
 *
 * Consistency semantics:
 * One - returns after first successful read, fails if all underlying reads fail
 * Quorum - returns after at least N/2 + 1 reads succeed and return the same value, fails otherwise
 * All - returns if all N reads succeed and return the same value, fails otherwise
 */
class TunableReplicatedReadableStore[-K, +V](
  stores: Seq[ReadableStore[K, V]], readConsistency: ConsistencyLevel
) extends AbstractReadableStore[K, V] {

  def doGet(k: K): Future[(Option[V], Seq[Int])] = {
    if (stores.isEmpty) {
      Future.value((None, List[Int]()))
    } else {
      val expected = readConsistency.expectedSuccesses(stores)
      // a map of counts per result value
      val counts = new ConcurrentHashMap[Option[V], AtomicInteger].asScala
      val successNodes = java.util.Collections.newSetFromMap(
        new ConcurrentHashMap[Int, java.lang.Boolean]).asScala

      val success = new AtomicInteger(0)
      val fail = new AtomicInteger(0)
      val futures = stores.map { s => s.get(k) }
      val promise = Promise.interrupts[(Option[V], Seq[Int])](futures: _*)
      for ((f, i) <- futures.zipWithIndex) {
        f.onSuccess { result =>
          success.getAndIncrement
          successNodes.add(i)
          val count = counts.get(result) match {
            case Some(existing) => existing.incrementAndGet
            case None =>
              val newCounter = new AtomicInteger(1)
              counts.putIfAbsent(result, newCounter) match {
                case Some(existing) => existing.incrementAndGet
                case None => newCounter.get
              }
          }
          // return result along with list of stores that succeeded
          if (count >= expected) promise.updateIfEmpty(Return((result, successNodes.toSeq)))
        } onFailure { e =>
          if (fail.incrementAndGet > stores.size - expected) {
            promise.updateIfEmpty(Throw(new ReadFailedException(k)))
          }
        } ensure {
          if (success.get + fail.get >= stores.size) {
            promise.updateIfEmpty(Throw(new ReadFailedException(k)))
          }
        }
      }
      promise
    }
  }

  override def get(k: K): Future[Option[V]] = {
    doGet(k).map { _._1 }
  }
}

/** Factory method to create TunableReplicatedStore instances */
object TunableReplicatedStore {
  def fromSeq[K, V](
    replicas: Seq[Store[K, V]],
    readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel,
    readRepair: Boolean = false,
    writeRollback: Boolean = false
  ): Store[K, V] = {
    new TunableReplicatedStore[K, V](
        replicas, readConsistency, writeConsistency, readRepair, writeRollback) {
      override def close(time: Time) = Future.collect(replicas.map(_.close(time))).unit
    }
  }
}

/**
 * Replicates writes to a seq of stores, and returns after picked write consistency is satisfied.
 *
 * Consistency semantics:
 * One - returns after first successful write (other writes can complete in the background),
 * fails if no write is successful
 * Quorum - returns after N/2 + 1 writes succeed (other writes can complete in the background),
 * fails otherwise
 * All - returns if all N writes succeed, fails otherwise
 *
 * Additionally, the following operations are supported:
 * readRepair - read repair any missing or incorrect data (only for Quorum reads)
 * writeRollback - delete the key on all replicas if write fails
 * (only for Quorum writes and All writes)
 */
class TunableReplicatedStore[-K, V](stores: Seq[Store[K, V]], readConsistency: ConsistencyLevel,
    writeConsistency: ConsistencyLevel, readRepair: Boolean = true, writeRollback: Boolean = true)
  extends TunableReplicatedReadableStore[K, V](stores, readConsistency)
  with Store[K, V] {

  override def get(k: K): Future[Option[V]] = {
    doGet(k).map { r : (Option[V], Seq[Int]) =>
      val (response, successes) = r
      if (readRepair && readConsistency == ConsistencyLevel.Quorum) {
        // optionally, perform read-repair (best effort)
        stores.zipWithIndex.foreach { case (s, i) =>
          if (!successes.contains(i)) s.put((k, response))
        }
      }
      response
    }
  }

  override def put(kv: (K, Option[V])): Future[Unit] = {
    if (stores.isEmpty) {
      Future.value(())
    } else {
      val expected = writeConsistency.expectedSuccesses(stores)
      val success = new AtomicInteger(0)
      val fail = new AtomicInteger(0)
      val futures = stores.map { s => s.put(kv) }
      val promise = Promise.interrupts[Unit](futures: _*)
      for (f <- futures) {
        f.onSuccess { result =>
          if (success.incrementAndGet >= expected) promise.updateIfEmpty(Return(()))
        } onFailure { e =>
          if (fail.incrementAndGet > stores.size - expected) {
            // optionally delete key in all replicas as part of rollback (best effort)
            if (writeRollback && (
              writeConsistency == ConsistencyLevel.Quorum ||
                writeConsistency == ConsistencyLevel.All)) {
              stores.foreach { s => s.put((kv._1, None)) }
            }
            promise.updateIfEmpty(Throw(new WriteFailedException(kv._1)))
          }
        } ensure {
          if (success.get + fail.get >= stores.size) promise.updateIfEmpty(Return(()))
        }
      }
      promise
    }
  }
}
