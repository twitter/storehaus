package com.twitter.storehaus.cascading.split

import com.twitter.storehaus.{IterableStore, ReadableStore, ReadableStoreProxy}
import com.twitter.concurrent.Spool
import com.twitter.util.Await
import org.apache.hadoop.io.LongWritable
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * IterableStores can be split up.
 * 
 * Wastes some resources, because default implementation needs to eagerly walk to position of store  
 */
class SplittableIterableStore[K, V, T <: SplittableIterableStore[K, V, T, U], U <: IterableStore[K, V] with ReadableStore[K, V]] (store: U, val position: Long = 0l, count: Long = 16384l) 
    extends SplittableStore[K, V, LongWritable, T]
    with ReadableStoreProxy[K, V] {

  override def self: U = store
  
  override def getSplits(numberOfSplitsHint: Int): Seq[T] = {
    val buffer = new ArrayBuffer[T]
    @tailrec def getSplits(number: Int, pos: Long): Unit = {
      buffer += getSplit(new LongWritable(pos))
      if (number > 0) getSplits(number - 1, pos + count)
    }
    // we just assume there is enough data available to do this, 
    // otherwise getAll will return an empty spool for some splits 
    getSplits(numberOfSplitsHint, position)
    buffer
  }

  override def getSplit(pos: LongWritable): T = {
    new SplittableIterableStore[K, V, T, U](store, pos.get(), count).asInstanceOf[T]
  }
   
  /**
   * enumerates keys in this SplittableStore
   */
  override def getAll: Spool[(K, V)] = {
    // eager seek to position :( (better to overwrite this ...)
    var spool = Await.result(store.getAll)
    var pos = 0l
    var head = null
    while (pos < position && !spool.isEmpty) {
      spool = head **:: spool
      pos += 1
    }
    // TODO: prevent reading after count reads
    spool
  }
  
  override def getInputSplits(stores: Seq[T], tapid: String): Array[SplittableStoreInputSplit[K, V, LongWritable]] =
    stores.map(sto => new SplittableStoreInputSplit[K, V, LongWritable](tapid, new LongWritable(sto.position))).toArray
}
