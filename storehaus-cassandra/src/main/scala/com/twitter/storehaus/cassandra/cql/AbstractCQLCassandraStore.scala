package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Future, FuturePool, Throw, Return}
import java.util.concurrent.Executors

abstract class AbstractCQLCassandraStore[K, V] (poolSize: Int, columnFamily: CQLCassandraConfiguration.StoreColumnFamily) 
  extends QueryableStore[String, (K, V)] 
  with CassandraCascadingRowMatcher[K, V]
  with IterableStore[K, V] {

  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))
  
  /**
   * takes a where condition-string (and where condition only) of a CQL query 
   * (...similar to but not equal to SQL) as parameters and executes a query from it. 
   */
  override def queryable: ReadableStore[String, Seq[(K, V)]] = new Object with ReadableStore[String, Seq[(K, V)]] {
    import scala.collection.JavaConverters._
    override def get(whereCondition: String): Future[Option[Seq[(K, V)]]] = futurePool {
      val qStart = QueryBuilder.select(getColumnNamesString).from(columnFamily.getPreparedNamed).getQueryString()
      val query = if((whereCondition eq null) || ("" == whereCondition)) qStart else s"$qStart WHERE $whereCondition"  
  	  val rSet = columnFamily.session.getSession.execute(query)
  	  if(rSet.isExhausted) None else {
  		Some(rSet.all().asScala.map(row => getKeyValueFromRow(row)))
  	  }
    }
  }
  
  /**
   * allows iteration over all (K, V) using a query against queryable
   */
  override def getAll: Future[Spool[(K, V)]] = queryable.get("").transform {
    case Throw(y) => Future.exception(y)
    case Return(x) => futurePool {
      val seq = x.getOrElse(Seq[(K, V)]())
      seq.foldRight(Spool.empty[(K, V)])((kv, spool) => (kv) **:: spool)
    }
  }
}
