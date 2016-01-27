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

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, SimpleStatement, Statement}
import com.datastax.driver.core.policies.{Policies, RoundRobinPolicy, ReconnectionPolicy, RetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Insert, QueryBuilder, Update}
import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, ReadableStoreProxy, Store, WithPutTtl}
import com.twitter.util.{Await, Closable, Future, Duration, FuturePool, Promise, Try, Throw, Return}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.Executors
import shapeless.{Generic, HList}
import shapeless.ops.hlist.{Mapper, ToList, Tupler}
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.traversable.traversableOps

object CQLCassandraStoreTupleValues {
  import AbstractCQLCassandraCompositeStore._
  
  def createColumnFamily[K : CassandraPrimitive, VS <: HList, MVResult <: HList] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnNames: List[String],
		valueSerializers: VS,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      )(implicit vs2str: CassandraPrimitivesToStringlist[VS]) = {
    createColumnFamilyWithToken[K, VS, MVResult, String](columnFamily, valueColumnNames, valueSerializers, None, "", keyColumnName)
  }

  def createColumnFamilyWithToken[K : CassandraPrimitive, VS <: HList, MVResult <: HList, T] (
		columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		valueColumnNames: List[String],
		valueSerializers: VS,
		tokenSerializer: Option[CassandraPrimitive[T]],
		tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME,
		keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME
      )(implicit vs2str: CassandraPrimitivesToStringlist[VS]) = {
    val valSerStrings = valueSerializers.stringlistify
    val keySerializer = implicitly[CassandraPrimitive[K]]
    columnFamily.session.createKeyspace
    val stmt = "CREATE TABLE IF NOT EXISTS " + columnFamily.getPreparedNamed +
	        " ( \"" + keyColumnName + "\" " + keySerializer.cassandraType + " PRIMARY KEY, " + 
	        AbstractCQLCassandraCompositeStore.createColumnListing(valueColumnNames, valSerStrings) + 
	        (tokenSerializer match {
	        	case Some(tokenSer) => ", \"" + tokenColumnName + "\" " + tokenSer.cassandraType
	        	case _ => ""
    		}) + ");"
    columnFamily.session.getSession.execute(stmt)
  }
}

/**
 * Simple key-value store, taking tuples as values. Entries must be of type Option (None represents that the
 * values should/have not be/been set, while Some represents a set value). This is needed as in Cassandra 
 * Columns may not be present in the column family. HLists must be provided to allow abstraction over arity 
 * of tuples.
 */
class CQLCassandraStoreTupleValues[K: CassandraPrimitive, V <: Product, VL <: HList, VS <: HList] (
		override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
		val valueColumnNames: List[String],
		val valueSerializers: VS,
		val keyColumnName: String = CQLCassandraConfiguration.DEFAULT_KEY_COLUMN_NAME,
		val consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
		override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
		val batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
		val ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
		   implicit ev2: Generic.Aux[V, VL],
		   ev3: Tupler.Aux[VL, V],
		   vAsList: ToList[VL, Any], 
		   val vsAsList: ToList[VS, CassandraPrimitive[_]],
		   vsFromList: FromTraversable[VL])
	extends AbstractCQLCassandraSimpleStore[K, V](poolSize, columnFamily, keyColumnName, consistency, batchType, ttl)
	with Store[K, V]
    with WithPutTtl[K, V, CQLCassandraStoreTupleValues[K, V, VL, VS]]
    with QueryableStore[String, (K, V)]
    with IterableStore[K, V]
    with CassandraCASStore[K, V] {
  
  val zippedColumnInfo = valueColumnNames.zip(valueSerializers.toList)
  
  override def withPutTtl(ttl: Duration): CQLCassandraStoreTupleValues[K, V, VL, VS] = new CQLCassandraStoreTupleValues[K, V, VL, VS](columnFamily, 
      valueColumnNames, valueSerializers, keyColumnName, consistency, poolSize, batchType, Some(ttl))

  protected def deleteColumns: Option[String] = Some(valueColumnNames.mkString(" , "))
  
  /**
   * puts will not delete single columns if they are set to None. But it will delete all columns if the
   * hole Option[TupleX] is None. 
   */
  protected def createPutQuery[K1 <: K](kv: (K1, V)): Insert = 
    createPutQuery(kv, QueryBuilder.insertInto(columnFamily.getPreparedNamed)).value(keyColumnName, implicitly[CassandraPrimitive[K]].toCType(kv._1))
  
  /**
   * creates a put query without key column
   */
  protected def createPutQuery[K1 <: K, BS <: BuiltStatement](kv: (K1, V), bs: BS): BS = {
    def toCType[T](serializer: CassandraPrimitive[T], value: T) = serializer.toCType(value)
    val sets = ev2.to(kv._2).toList.zip(zippedColumnInfo)
    sets.foldLeft(bs)((insUpd, values) => values._1 match {
      case Some(v) => insUpd match {
        case insert: Insert => 
          insert.value(s""""${values._2._1}"""", toCType(values._2._2.asInstanceOf[CassandraPrimitive[Any]], v)).asInstanceOf[BS]
        case ass: Update.Assignments =>
          ass.and(QueryBuilder.set(s""""${values._2._1}"""", toCType(values._2._2.asInstanceOf[CassandraPrimitive[Any]], v))).asInstanceOf[BS]
      }
      case None => insUpd
    })
  }
  
  override def getKeyValueFromRow(row: Row): (K, V) = (keySerializer.fromRow(row, keyColumnName).get, getRowValue(row).tupled)
  
  /**
   * get will return a tupleX with entries of type Option[T_x], each being None on unavailable or Some on stored 
   */
  override protected def getValue(result: ResultSet): Option[V] = Some(getRowValue(result.one()).tupled)
  
  protected def getRowValue(row: Row): VL = {
    valueSerializers.toList.zip(valueColumnNames).map(sercol => sercol._1.fromRow(row, s""""${sercol._2}"""")).toHList[VL].get
  }
      
  /**
   * return comma separated list of key and value column names
   */
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    AbstractCQLCassandraCompositeStore.quote(sb, keyColumnName, true)
    valueColumnNames.map(name => AbstractCQLCassandraCompositeStore.quote(sb, name, name != valueColumnNames.last))
    sb.toString
  }
  
  override def getCASStore[T](tokenColumnName: String = CQLCassandraConfiguration.DEFAULT_TOKEN_COLUMN_NAME)(
      implicit equiv: Equiv[T], cassTokenSerializer: CassandraPrimitive[T], tokenFactory: TokenFactory[T]): CASStore[T, K, V] 
      with IterableStore[K, V] with Closable = new CQLCassandraStoreTupleValues[K, V, VL, VS](columnFamily, valueColumnNames,
          valueSerializers, keyColumnName, consistency, poolSize, batchType, ttl) with CassandraCASStoreSimple[T, K, V] 
      with ReadableStore[K, V]  with Closable  {
    override protected def deleteColumns: Option[String] = Some(s"${super.deleteColumns} , $tokenColumnName")
    override protected def createPutQuery[K1 <: K](kv: (K1, V)) = super.createPutQuery(kv)    
    override def cas(token: Option[T], kv: (K, V))(implicit ev1: Equiv[T]): Future[Boolean] = { 
      def putQueryConversion(kv: (K, Option[V])): BuiltStatement = token match {
        case None => createPutQuery[K]((kv._1, kv._2.get)).value(tokenColumnName, cassTokenSerializer.toCType(tokenFactory.createNewToken)).ifNotExists()
        case Some(tok) => createPutQuery[K, Update.Assignments]((kv._1, kv._2.get), QueryBuilder.update(columnFamily.getPreparedNamed).`with`).
        and(QueryBuilder.set(tokenColumnName, cassTokenSerializer.toCType(tokenFactory.createNewToken))).
        where(QueryBuilder.eq(keyColumnName, implicitly[CassandraPrimitive[K]].toCType(kv._1))).
        onlyIf(QueryBuilder.eq(tokenColumnName, cassTokenSerializer.toCType(tok)))
      }  
      casImpl(token, kv, putQueryConversion(_), tokenFactory, tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def get(key: K)(implicit ev1: Equiv[T]): Future[Option[(V, T)]] = {
	    def rowExtractor(r: Row): V = super.getRowValue(r).tupled
	    getImpl(key, createGetQuery(_), cassTokenSerializer, rowExtractor(_), tokenColumnName, columnFamily, consistency)(ev1)
    }
    override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = super[ReadableStore].multiGet(ks)
  }
}
