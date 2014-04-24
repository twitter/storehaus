package com.twitter.storehaus.cassandra

import com.twitter.util.{ Future, ExecutorServiceFuturePool }
import me.prettyprint.hector.api.{ Serializer, Keyspace }
import me.prettyprint.hector.api.beans.{ AbstractComposite, HColumn }
import me.prettyprint.hector.api.factory.HFactory
import javax.naming.OperationNotSupportedException
import scala.collection.JavaConversions._

/**
 * abstraction over Cassandra's slice query using PartiallyIterableStore
 */
trait CassandraPartiallyIterableStore[RK, CK, RS, CS, V, C <: AbstractComposite] 
		extends PartiallyIterableStore[(RK, CK), V] {

  // methods used to pull in state information
  def createColKey(key: CK): C  
  def createRowKey(key: RK): C  
  def getCompositeSerializer: Serializer[C]
  def getValueSerializer: Serializer[V]
  def getRowResult(list: List[AbstractComposite#Component[_]]): RK
  def getColResult(list: List[AbstractComposite#Component[_]]): CK
  def getFuturePool: ExecutorServiceFuturePool
  def getKeyspace: Keyspace
  def getColumnFamilyName: String
  
  /** 
   *  Abstract Cassandra code for slice queries on stores using composites
   *  TODO: (future version) do paging and don't return a List but a lazy Iterable (or Spool)
   */
  override def multiGet(start: (RK, CK), end: Option[(RK, CK)] = None, maxNumKeys: Option[Int] = None, descending: Boolean = false): Future[Iterable[((RK, CK), V)]] = {
    import scala.collection.JavaConversions._
    val (rkStart, ckStart) = start
    val rowKeyStart = createRowKey(rkStart)
    val colKeyStart = createColKey(ckStart)
    getFuturePool {
      val query = HFactory.createSliceQuery(getKeyspace, getCompositeSerializer, getCompositeSerializer, getValueSerializer)
        .setColumnFamily(getColumnFamilyName)
        .setKey(rowKeyStart)
      end match {
        case Some((rkEnd, ckEnd)) => {
          val rowKeyEnd = createRowKey(rkEnd)
          val colKeyEnd = createColKey(ckEnd)
          if(rowKeyEnd != rowKeyStart) throw new OperationNotSupportedException("Cassandra can only do slice queries on the same row key")
          else query.setRange(
              if (!descending) colKeyStart else colKeyEnd,
              if (!descending) colKeyEnd else colKeyStart,
              descending,
              maxNumKeys.getOrElse(Int.MaxValue))
        }
        case None => {
          query.setRange(
              colKeyStart,
              null.asInstanceOf[C],
              false,
              maxNumKeys.getOrElse(Int.MaxValue))          
        }
      }
      val result = query.execute()
      result.get().getColumns().toList.map{(hcol: HColumn[C, V]) =>
        ((getRowResult(iterableAsScalaIterable(rowKeyStart.getComponents).toList), 
          getColResult(iterableAsScalaIterable(hcol.getName.getComponents).toList)), 
            hcol.getValue)}.toIterable
    }
  }
}
