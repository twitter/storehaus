package com.twitter.storehaus.caliper

import com.google.caliper.{Param, SimpleBenchmark}
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.bijection._
import com.twitter.storehaus._
import com.twitter.conversions.time._
import com.twitter.algebird._
import com.twitter.storehaus.cache._

import scala.math.pow
import com.twitter.storehaus.algebra._
import com.twitter.util.{Duration, Await, Future}

class DelayedStore[K, V](val self: Store[K, V])(implicit timer: com.twitter.util.Timer) extends StoreProxy[K, V] {
  override def put(kv: (K, Option[V])): Future[Unit] = {
    self.put(kv).delayed(10.milliseconds)
  }

  override def get(kv: K): Future[Option[V]] = {
    self.get(kv).delayed(10.milliseconds)
  }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] =
    self.multiGet(ks).map { case (k,v) =>
      (k, v.delayed(10.milliseconds))
    }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] =
    self.multiPut(kvs).map { case (k,v) =>
      (k, v.delayed(10.milliseconds))
    }
}

class WriteThroughCacheBenchmark extends SimpleBenchmark {
  implicit val custTimer = new com.twitter.util.ScheduledThreadPoolTimer(20)
  import StoreAlgebra._

  implicit val hllMonoid = new HyperLogLogMonoid(14)

  @Param(Array("100", "1000", "10000"))
  var numInputKeys: Int = 0

  @Param(Array("10000", "100000"))
  var numElements: Int = 0

  var inputData: Seq[Map[Long, HLL]] = _

  var store: MergeableStore[Long, HLL] = _

  var noCacheStore: MergeableStore[Long, HLL] = _

  override def setUp {
    val rng = new scala.util.Random(3)
    val byteEncoder = implicitly[Injection[Long, Array[Byte]]]
    def setSize = rng.nextInt(10) + 1 // 1 -> 10
    def hll(elements: Set[Long]): HLL = hllMonoid.batchCreate(elements)(byteEncoder)
    val inputIntermediate = (0L until numElements).map {_ =>
      val setElements = (0 until setSize).map{_ => rng.nextInt(1000).toLong}.toSet
      (pow(numInputKeys, rng.nextFloat).toLong, hll(setElements))
    }.grouped(20)

    inputData = inputIntermediate.map(s => MapAlgebra.sumByKey(s)).toSeq

    val delayedStore = new DelayedStore(new ConcurrentHashMapStore[Long, HLL])
    val hhStore = HHFilteredStore.buildStore[Long, HLL](
      new ConcurrentHashMapStore[Long, HLL],
      MutableCache.ttl(Duration.fromSeconds(10000), numElements),
      HeavyHittersPercent(0.5f),
      WriteOperationUpdateFrequency(1),
      RollOverFrequencyMS(10000000L))
    store = new WriteThroughStore(delayedStore, hhStore).toMergeable
    noCacheStore = delayedStore.toMergeable
  }

  def timeDoUpdates(reps: Int): Int = {
    Await.result(Future.collect((0 until reps).flatMap { indx =>
      inputData.map(d => FutureOps.mapCollect(store.multiMerge(d)).unit)
    }.toSeq))
    12
  }

  def timeDoUpdatesWithoutCache(reps: Int): Int = {
    Await.result(Future.collect((0 until reps).flatMap { indx =>
      inputData.map(d => FutureOps.mapCollect(noCacheStore.multiMerge(d)).unit)
    }.toSeq))
    12
  }
}
