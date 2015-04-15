/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement, Token}
import com.datastax.driver.core.policies.{Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, QueryBuilder, Insert, Update}
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, ReadableStoreProxy, Store, WithPutTtl}
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher
import com.twitter.util.{Await, Future, Duration, FuturePool, Promise, Try, Throw, Return}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors
import java.util.{ Date, List => JList, Map => JMap, Set => JSet, UUID }
import java.nio.ByteBuffer
import scala.annotation.tailrec
import com.twitter.storehaus.cascading.Instance
import com.datastax.driver.core.ColumnDefinitions
import java.math.{BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import java.net.InetAddress
import com.datastax.driver.core.TupleValue
import com.datastax.driver.core.UDTValue
import com.google.common.reflect.TypeToken

object CQLCassandraRowStore {
  
  def createColumnFamily[K : CassandraPrimitive] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
	    columns: List[(String, CassandraPrimitive[_])],
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      ) = {
    createColumnFamilyWithToken[K, String](columnFamily, None, columns, "", keyColumnName)
  }

  def createColumnFamilyWithToken[K : CassandraPrimitive, T] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		tokenSerializer: Option[CassandraPrimitive[T]],
		columns: List[(String, CassandraPrimitive[_])],
		tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      ) = {
    val keySerializer = implicitly[CassandraPrimitive[K]]
    columnFamily.session.createKeyspace
    val stmt = "CREATE TABLE IF NOT EXISTS " + columnFamily.getPreparedNamed +
	        " ( " + columns.map(col => " \"" + col._1 + "\" " + col._2.cassandraType + 
	            (if(col._1 == keyColumnName) " PRIMARY KEY" else "")).mkString(",") + 
	        (tokenSerializer match {
	        	case Some(tokenSer) => ", \"" + tokenColumnName + "\" " + tokenSer.cassandraType
	        	case _ => ""
    		}) + ");"
    columnFamily.session.getSession.execute(stmt)
  }
}


/**
 * Simple key-value store, which uses simple Cassandra-Rows as values.
 * A single column is distinguished as to be the key.
 */
class CQLCassandraRowStore[K : CassandraPrimitive] (
		override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		val columns: List[(String, CassandraPrimitive[_])], // this list includes the key-column
		val keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
		val consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
		val ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)
	extends AbstractCQLCassandraSimpleStore[K, Row](poolSize, columnFamily, keyColumnName, consistency, batchType, ttl)
	with Store[K, Row] 
    with WithPutTtl[K, Row, CQLCassandraRowStore[K]] 
    with QueryableStore[String, (K, Row)] 
    with IterableStore[K, Row] 
    with CassandraCASStore[K, Row] {
  
  override def withPutTtl(ttl: Duration): CQLCassandraRowStore[K] = new CQLCassandraRowStore(columnFamily, 
      columns, keyColumnName, consistency, poolSize, batchType, Some(ttl))
  
  protected def deleteColumns: Option[String] = None
  
  @tailrec final def recursiveAddValues[T](cols: List[(String, CassandraPrimitive[_])], 
          bs: BuiltStatement, row: Row, token: Option[CassandraTokenInformation[T]] = None): BuiltStatement = {
    def includeToken = token.map { tok => bs match {
        case ins: Insert => ins.value(tok.tokenColumn, tok.cassTokenSerializer.toCType(tok.token))
        case upd: Update => upd.`with`(QueryBuilder.set(tok.tokenColumn, tok.cassTokenSerializer.toCType(tok.token)))
        case ass: Update.Assignments => ass.and(QueryBuilder.set(tok.tokenColumn, tok.cassTokenSerializer.toCType(tok.token)))
      }
    }.getOrElse(bs)
    def getKeyValue = implicitly[CassandraPrimitive[K]].fromRow(row, keyColumnName)
    def internalValue[T](name: String, cass: CassandraPrimitive[T], value: Option[T]): BuiltStatement = bs match {
      case ins: Insert => ins.value(name, cass.toCType(value.get))
      case upd: Update => if(name == keyColumnName) {
          bs
        } else {
          upd.`with`(QueryBuilder.set(name, cass.toCType(value.get)))
        }
      case ass: Update.Assignments => if(name == keyColumnName) {
          bs
        } else {
          ass.and(QueryBuilder.set(name, cass.toCType(value.get)))
        }
      }
    if(cols.isEmpty) {
      (includeToken match {
        case upd: Update => upd.where(QueryBuilder.eq(keyColumnName, implicitly[CassandraPrimitive[K]].toCType(getKeyValue.get)))
        case ass: Update.Assignments => ass.where(QueryBuilder.eq(keyColumnName, implicitly[CassandraPrimitive[K]].toCType(getKeyValue.get)))
        case _ => bs
      })
    } else {
      val name = cols.head._1
      val optVal = cols.head._2.fromRow(row, name)
      val ins2 = optVal match {
        case None => bs
        case Some(value) => internalValue(name, cols.head._2.asInstanceOf[CassandraPrimitive[Any]], optVal)
      }
      recursiveAddValues(cols.tail, ins2, row, token)
    } 
  }
  
  protected def createPutQuery[K1 <: K](kv: (K1, Row)): Insert = {
    val (key, row) = kv
    recursiveAddValues(columns, QueryBuilder.insertInto(columnFamily.getPreparedNamed), row).asInstanceOf[Insert]
  }
  
  override def getKeyValueFromRow(row: Row): (K, Row) = (keySerializer.fromRow(row, keyColumnName).get, row)
  
  /**
   * return comma separated list of key and value column name
   */
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    columns.foreach(col => AbstractCQLCassandraCompositeStore.quote(sb, col._1, true))
    AbstractCQLCassandraCompositeStore.quote(sb, keyColumnName)
    sb.toString
  }
  
  override def getValue(result: ResultSet): Option[Row] = Option(result.one())
  
  /**
   * we assume the tokemColumns is part of columns, so there is not so much to do, here
   */
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(
      implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, K, Row] with IterableStore[K, Row] = 
        new CQLCassandraRowStore[K](columnFamily, columns, keyColumnName, consistency, poolSize, batchType, ttl) with CassandraCASStoreSimple[T, K, Row]
        with ReadableStore[K, Row] {
    override protected def createPutQuery[K1 <: K](kv: (K1, Row)) = super.createPutQuery(kv)    
    override def cas(token: Option[T], kv: (K, Row))(implicit ev1: Equiv[T]): Future[Boolean] = { 
      def putQueryConversion(kv: (K, Option[Row])): BuiltStatement = token match {
        case None => createPutQuery[K](kv._1, kv._2.get).ifNotExists()
        case Some(token) => recursiveAddValues[T](columns, QueryBuilder.update(columnFamily.getPreparedNamed), kv._2.get).asInstanceOf[Update.Assignments].
          onlyIf(QueryBuilder.eq(tokenColumnName, token))
      } 
      casImpl(token, kv, putQueryConversion(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def get(key: K)(implicit ev1: Equiv[T]): Future[Option[(Row, T)]] =
      getImpl(key, createGetQuery(_), cassTokenSerializer, row => row, tokenColumnName, columnFamily, consistency)(ev1)
    override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[Row]]] = super[ReadableStore].multiGet(ks)
  }
}

/**
 * simple row to store values. CassandraPrimitives currently only use gets with names,
 * so we only implement these, as setters won't make sense with a List[] val in the constructor.
 * Note: this is only to store values, a .get on CQLCassandraRowStore will always return
 * a "real" Row from the datastax driver and never CQLCassandraRow
 */
class CQLCassandraRow(columns: Map[String, _]) extends Row {
  import scala.collection.JavaConverters._
  private def get[T](name: String) = columns.get(name).get.asInstanceOf[T]
  override def getColumnDefinitions(): ColumnDefinitions = ???
  override def isNull(i: Int): Boolean = ???
  override def isNull(name: String): Boolean = columns.get(name).isEmpty
  override def getBool(i: Int): Boolean = ???
  override def getBool(name: String): Boolean = get[Boolean](name)
  override def getInt(i: Int): Int = ???
  override def getInt(name: String): Int = get[Int](name)
  override def getLong(i: Int): Long = ???
  override def getLong(name: String): Long = get[Long](name)
  override def getDate(i: Int): Date = ???
  override def getDate(name: String): Date = get[Date](name)
  override def getFloat(i: Int): Float = ???
  override def getFloat(name: String): Float = get[Float](name)
  override def getDouble(i: Int): Double = ???
  override def getDouble(name: String): Double = get[Double](name)
  override def getBytesUnsafe(i: Int): ByteBuffer = ???
  override def getBytesUnsafe(name: String): ByteBuffer = ByteBuffer.wrap(get[Array[Byte]](name))
  override def getBytes(i: Int): ByteBuffer = ???
  override def getBytes(name: String): ByteBuffer = ByteBuffer.wrap(get[Array[Byte]](name))
  override def getString(i: Int): String = ???
  override def getString(name: String): String = get[String](name)
  override def getVarint(i: Int): JBigInteger = ???
  override def getVarint(name: String): JBigInteger = get[JBigInteger](name)
  override def getDecimal(i: Int): JBigDecimal = ???
  override def getDecimal(name: String): JBigDecimal = get[JBigDecimal](name)
  override def getUUID(i: Int): UUID = ???
  override def getUUID(name: String): UUID = get[UUID](name)
  override def getInet(i: Int): InetAddress = ???
  override def getInet(name: String): InetAddress = get[InetAddress](name)
  override def getList[T](i: Int, elementsClass: Class[T]): JList[T] = ???
  override def getList[T](name: String, elementsClass: Class[T]): JList[T] = get[List[T]](name).asJava
  override def getSet[T](i: Int, elementsClass: Class[T]): JSet[T] = ???
  override def getSet[T](name: String, elementsClass: Class[T]): JSet[T] = get[Set[T]](name).asJava
  override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] = ???
  override def getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V]): JMap[K, V] = get[Map[K, V]](name).asJava
  override def getTupleValue(i: Int): TupleValue = ???
  override def getTupleValue(name: String): TupleValue = get[TupleValue](name)
  override def getUDTValue(i: Int): UDTValue = ???
  override def getUDTValue(name: String): UDTValue = get[UDTValue](name)
  override def getList[T](i: Int, tt: TypeToken[T]): JList[T] = ???
  override def getMap[K, V](i: Int, tt1: TypeToken[K], tt2: TypeToken[V]): JMap[K,V] = ???
  override def getSet[T](i: Int, tt: TypeToken[T]): JSet[T] = ???
  override def getList[T](name: String, tt: TypeToken[T]): JList[T] = get[List[T]](name).asJava
  override def getMap[K, V](name: String, tt1: TypeToken[K], tt2: TypeToken[V]): JMap[K,V] = get[Map[K, V]](name).asJava
  override def getSet[T](name: String, tt: TypeToken[T]): JSet[T] = get[Set[T]](name).asJava
  override def getPartitionKeyToken(): Token = ???
  override def getToken(name: String): Token = get[Token](name)
  override def getToken(i: Int): Token = ???
}

