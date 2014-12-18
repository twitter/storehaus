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
class SplittableIterableStore[K, V, U <: IterableStore[K, V] with ReadableStore[K, V]] (store: U, val position: Long = 0l, count: Long = 16384l, 
    val version: Option[Long] = None) extends SplittableStore[K, V, LongWritable] with ReadableStoreProxy[K, V] {

  override def self: U = store
  
  def getWritable: LongWritable = new LongWritable(position)
  
  override def getSplits(numberOfSplitsHint: Int): Seq[SplittableStore[K, V, LongWritable]] = {
    val buffer = new ArrayBuffer[SplittableStore[K, V, LongWritable]]
    @tailrec def getSplits(number: Int, pos: Long): Unit = if (number > 0) {
      buffer += getSplit(new LongWritable(pos), version)
      getSplits(number - 1, pos + count)
    }
    // we just assume there is enough data available to do this, 
    // otherwise getAll will return an empty spool for some splits 
    getSplits(numberOfSplitsHint, position)
    buffer
  }

  override def getSplit(pos: LongWritable, version: Option[Long]): SplittableStore[K, V, LongWritable] = {
    new SplittableIterableStore[K, V, U](store, pos.get(), count, version)
  }
   
  /**
   * enumerates keys in this SplittableStore
   */
  override def getAll: Spool[(K, V)] = {
    def spoolToCountedSpool[K, V](spool: Spool[(K, V)], counter: Long): Future[Spool[(K, V)]] = Future.value {
      if ((!spool.isEmpty) && (counter > 0)) 
        spool.head *:: spoolToCountedSpool(Await.result(spool.tail), counter - 1l)
      else
        Spool.empty
    }
    // eager seek to position :( (should be overwritten in 
    // specializations if seeking is allowed, there)
    val spool = Await.result(store.getAll)
    // take only supports Ints so we seek ourselves
    @tailrec def seek(position: Long, spool: Spool[(K, V)]): Spool[(K, V)] = {
      if(position == 0) spool
      else {
        if(spool.isEmpty) spool
        else seek(position -1l, Await.result(spool.tail))
      }
    }
    Await.result(spoolToCountedSpool(seek(position, spool), count))
  }
  
  override def getInputSplits(stores: Seq[SplittableStore[K, V, LongWritable]], tapid: String, version: Option[Long]): Array[SplittableStoreInputSplit[K, V, LongWritable]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, LongWritable](tapid, sto.getWritable, version)).toArray
}

