package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.{IterableStore, ReadableStore, ReadableStoreProxy}
import com.twitter.concurrent.Spool
import com.twitter.util.{Await, Future}
import org.apache.hadoop.io.LongWritable
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * IterableStores can be split up.
 * 
 * Wastes some resources, because default implementation needs to eagerly walk to position of store  
 */
class SplittableIterableStore[K, V, U <: IterableStore[K, V] with ReadableStore[K, V]] (store: U, val position: Long = 0l, count: Long = 16384l, val version: Option[Long] = None) 
    extends SplittableStore[K, V, LongWritable, SplittableIterableStore[K, V, U]]
    with ReadableStoreProxy[K, V] {

  override def self: U = store
  
  override def getSplits(numberOfSplitsHint: Int): Seq[SplittableIterableStore[K, V, U]] = {
    val buffer = new ArrayBuffer[SplittableIterableStore[K, V, U]]
    @tailrec def getSplits(number: Int, pos: Long): Unit = {
      buffer += getSplit(new LongWritable(pos), version)
      if (number > 0) getSplits(number - 1, pos + count)
    }
    // we just assume there is enough data available to do this, 
    // otherwise getAll will return an empty spool for some splits 
    getSplits(numberOfSplitsHint, position)
    buffer
  }

  override def getSplit(pos: LongWritable, version: Option[Long]): SplittableIterableStore[K, V, U] = {
    new SplittableIterableStore[K, V, U](store, pos.get(), count, version).asInstanceOf[SplittableIterableStore[K, V, U]]
  }
   
  /**
   * enumerates keys in this SplittableStore
   */
  override def getAll: Spool[(K, V)] = {
    def spoolToCountedSpool[K, V](spool: Spool[(K, V)], counter: Long): Future[Spool[(K, V)]] = Future.value {
      if ((!spool.isEmpty) && (counter > 0)) 
        spool.head *:: spoolToCountedSpool(spool, counter - 1l)
      else
        Spool.empty
    }
    // eager seek to position :( (should be overwritten in 
    // specializations if seeking is allowed, there)
    val spool = Await.result(store.getAll)
    // take only supports Ints so we seek ourselves
    @tailrec def seek(position: Long, spool: Spool[(K, V)]): Spool[(K, V)] = {
      if(position == 0) spool
      else seek(position -1l, Await.result(spool.tail))
    }
    Await.result(spoolToCountedSpool(seek(position, spool), count))
  }
  
  override def getInputSplits(stores: Seq[SplittableIterableStore[K, V, U]], tapid: String, version: Option[Long]): Array[SplittableStoreInputSplit[K, V, LongWritable]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, LongWritable](tapid, new LongWritable(sto.position), version)).toArray
}

