package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Future, FuturePool, Throw, Return}
import java.util.concurrent.{Executors, TimeUnit}
import org.slf4j.{ Logger, LoggerFactory }

abstract class AbstractCQLCassandraStore[K, V] (poolSize: Int, columnFamily: CQLCassandraConfiguration.StoreColumnFamily) 
  extends QueryableStore[String, (K, V)] 
  with CassandraCascadingRowMatcher[K, V]
  with IterableStore[K, V] {

  private val log = LoggerFactory.getLogger(classOf[AbstractCQLCassandraStore[K, V]])
  
  val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))

  // make sure stores are shut down, even when the JVM is going down
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run: Unit = {
      def forceShutdown: Unit = {
        val droppedTasks = futurePool.executor.shutdownNow()
        log.warn(s"""Forcing shutdown of futurePool because Timeout 
          of ${columnFamily.session.cluster.shutdownTimeout.inMillis} ms has been reached. 
          Dropping ${droppedTasks.size} tasks - potential data loss.""")        
      }
      futurePool.executor.shutdown()
      try {
        if (!futurePool.executor.awaitTermination(columnFamily.session.cluster.shutdownTimeout.inMillis, TimeUnit.MILLISECONDS)) {
          // time's up. Forcing shutdown
          forceShutdown
        }
      } catch {
        // handle requested cancel
        case e: InterruptedException => forceShutdown
      }
    }
  })  
  
  /**
   * takes a where condition-string (and where condition only) of a CQL query 
   * (...similar to but not equal to SQL) as parameters and executes a query from it. 
   */
  override def queryable: ReadableStore[String, Seq[(K, V)]] = new Object with ReadableStore[String, Seq[(K, V)]] {
    import scala.collection.JavaConverters._
    override def get(whereCondition: String): Future[Option[Seq[(K, V)]]] = futurePool {
      val qStart = QueryBuilder.select(getColumnNamesString.split(","):_*).from(columnFamily.getPreparedNamed).getQueryString()
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
