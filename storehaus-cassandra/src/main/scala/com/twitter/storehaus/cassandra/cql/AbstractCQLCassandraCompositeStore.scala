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

import com.twitter.util.{Duration, Future, FuturePool, Time}
import com.twitter.concurrent.Spool
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, ResultSet, Row, Statement}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, ReconnectionPolicy, RetryPolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.{BuiltStatement, Clause, QueryBuilder, Update, Select}
import com.twitter.storehaus.{IterableStore, QueryableStore, ReadableStore, Store}
import com.websudos.phantom.CassandraPrimitive
import java.util.concurrent.{Executors, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import shapeless.{HList, HNil, ::, DepFn1, Poly1}
import shapeless.ops.hlist.{Mapped, Mapper}
import org.slf4j.LoggerFactory
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.Clause.SimpleClause

object AbstractCQLCassandraCompositeStore {
  
  /**
   * type class to map an HList of CassandraPrimitives to a List[String]
   */
  trait CassandraPrimitivesToStringlist[L <: HList] {
    def apply(serializers: L): List[String]
  }
  object CassandraPrimitivesToStringlist {
    implicit def hnilcass2strings: CassandraPrimitivesToStringlist[HNil] = 
      new CassandraPrimitivesToStringlist[HNil] {
        override def apply(serializers: HNil): List[String] = List()
      }
    implicit def hlistcass2strtings[M <: CassandraPrimitive[_], K <: HList](
      implicit hl2strl: CassandraPrimitivesToStringlist[K]): CassandraPrimitivesToStringlist[M :: K] = 
        new CassandraPrimitivesToStringlist[M :: K] {
          override def apply(serializers: M :: K): List[String] = {
            serializers.head.cassandraType :: hl2strl(serializers.tail) 
          }
        }
  }
  implicit class HListCassandraPrimitives2StringList[K <: HList](serializers: K) {
    def stringlistify(implicit hl2strl: CassandraPrimitivesToStringlist[K]): List[String] = hl2strl(serializers)
  }

  /**
   * used to map over an example key to implicit serializers
   */
  object cassandraSerializerCreation extends Poly1 {
    implicit def default[T : CassandraPrimitive]: Case.Aux[T, CassandraPrimitive[T]] =
      at[T](_ => implicitly[CassandraPrimitive[T]])
  }

  /**
   * creates an HList of serializers (CasasandraPrimitives) given an example 
   * key HList and pulls serializers out of implicit scope.
   */
  def getSerializerHListByExample[KL <: HList](keys: KL)(implicit mapper: Mapper[cassandraSerializerCreation.type, KL]) : mapper.Out = {
    keys.map(cassandraSerializerCreation)
  }

  /**
   * helper trait for declaring the HList recursive function 
   * to append keys on an ArrayBuffer in a type safe way
   */
  trait Append2Composite[R <: ArrayBuffer[Clause], K <: HList, Q <: HList] {
    def apply(r: R, k: K, s: List[String], ser: Q, insert: Option[Insert]): Option[Insert]
  }

  /**
   * helper implicits for the recursion itself
   */
  object Append2Composite {
    implicit def hnilAppend2Composite[R <: ArrayBuffer[Clause]]: Append2Composite[R, HNil, HNil] = 
      new Append2Composite[R, HNil, HNil] {
        override def apply(r: R, k: HNil, s: List[String], ser: HNil, insert: Option[Insert]): Option[Insert] = insert
      }
    implicit def hlistAppend2Composite[R <: ArrayBuffer[Clause], M, K <: HList, Q <: HList](
      implicit a2c: Append2Composite[R, K, Q]): Append2Composite[R, M :: K, CassandraPrimitive[M] :: Q] =
        new Append2Composite[R, M :: K, CassandraPrimitive[M] :: Q] {
    	    override def apply(r: R, k: M :: K, s: List[String], ser: CassandraPrimitive[M] :: Q, insert: Option[Insert]): Option[Insert] = {
   	        val newInsert = insert match {
              case None =>
                r.add(QueryBuilder.eq(s"""\"${s.head}\"""", ser.head.toCType(k.head)))
                insert
              case Some(ins) => 
                Some(ins.value(s"""\"${s.head}\"""", ser.head.toCType(k.head)))
            }
    	      a2c(r, k.tail, s.tail, ser.tail, newInsert)
    	    }
        }
  }

  /**
   * recursive function callee implicit
   */
  implicit def append2Composite[R <: ArrayBuffer[Clause], K <: HList, S <: List[String], Q <: HList](r: R)(k: K, s: List[String], ser: Q, insert: Option[Insert] = None)
  	(implicit a2c: Append2Composite[R, K, Q]) = a2c(r, k, s, ser, insert) 
    
  /**
   * helper trait for declaring the HList recursive function 
   * to create a key from a row
   * S are the serializers for the key in an HList
   */
  trait Row2Result[S <: HList] {
    type Out
    def apply(row: Row, serializers: S, names: List[String]): Out
  }

  /**
   * helper implicits for the recursion itself
   */
  object Row2Result {
    type Aux[S <: HList, R <: HList] = Row2Result[S] { type Out = R }
    implicit def hnilRow2Result[S <: HNil]: Aux[S, HNil] = new Row2Result[S] {
      type Out = HNil
      override def apply(row: Row, serializers: S, names: List[String]): Out = HNil
    }
    implicit def hlistRow2Result[M, K <: HList, S <: HList](implicit r2r: Aux[S, K]): 
      Aux[CassandraPrimitive[M] :: S, M :: K] = 
        new Row2Result[CassandraPrimitive[M] :: S] {
          type Out = M :: K
    	    override def apply(row: Row, serializers: CassandraPrimitive[M] :: S, names: List[String]): Out = {
    	      val res = serializers.head.fromRow(row, names.head).get
    	      res :: r2r(row, serializers.tail, names.tail) 
    	    }
        }
  }

  /**
   * recursive function callee implicit
   */
  implicit def row2result[S <: HList](row: Row, serializers: S, names: List[String])
  	(implicit r2r: Row2Result[S]): r2r.Out = r2r(row, serializers, names) 

  /**
   * quote a column and append it to a StringBuilder
   */
  def quote(sb: StringBuilder, column: String, comma: Boolean = false): Unit = {
    sb.append(column.filterNot(_ == '"'))
    if(comma) sb.append(",")
  }
  	
  /** 
   *  provides a join method for Traversables,
   *  this is actually a fold with an initial function
   */
  implicit class Joinable[T](val traversable: Traversable[T]) {
    def join[R](initialFunction: T => R)(inbetweenFunction: (T, R) => R): R = {
      @tailrec def recJoin(acc: Option[R], traverse: Traversable[T]): R = {
        if (traverse.size == 0) return acc.get
        val result = acc match {
          case Some(accResult) => Some(inbetweenFunction(traverse.head, accResult))
          case None => Some(initialFunction(traverse.head))
        }
        recJoin(result, traverse.tail)
      }
      recJoin(None, traversable)  
    }
  }
  
  /**
   * create a String listing columns and their types; useful to create column families 
   */
  def createColumnListing(names: List[String], types: List[String]): String = names.size match {
    case 0 => ""
    case _ => "\"" + names.head.filterNot(_ == '"') + "\" " + types.head.filterNot(_ == '"') + ", " + createColumnListing(names.tail, types.tail)
  }
}

abstract class AbstractCQLCassandraCompositeStore[RK <: HList, CK <: HList, V, RS <: HList, CS <: HList] (
  override val columnFamily: CQLCassandraConfiguration.StoreColumnFamily,
  rowkeySerializer: RS,
  rowkeyColumnNames: List[String],
  colkeySerializer: CS,
  colkeyColumnNames: List[String],
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  override val poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
    implicit evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
    evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[RS, RK],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result.Aux[CS, CK],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS], 
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS])
  extends AbstractCQLCassandraStore[(RK, CK), V](poolSize, columnFamily)
  with Store[(RK, CK), V] 
  with QueryableStore[String, ((RK, CK), V)] 
  with IterableStore[(RK, CK), V] {
  import AbstractCQLCassandraCompositeStore._

  @transient private val log = LoggerFactory.getLogger(classOf[AbstractCQLCassandraCompositeStore[RK, CK, V, RS, CS]]) 
  
  log.debug(s"""Creating new AbstractCQLCassandraCompositeStore on ${columnFamily.session.cluster.hosts} with consistency=${consistency.name} and 
    load-balancing=${columnFamily.session.getCluster.getConfiguration.getPolicies.getLoadBalancingPolicy}""")
  
  protected def putValue[S <: BuiltStatement, U <: BuiltStatement, T](value: V, stmt: S, token: Option[T]): U  
  
  protected def putToken[S <: BuiltStatement, U <: BuiltStatement, T](tokenColumnName: String,
      tokenFactory: TokenFactory[T], cassTokenSerializer: CassandraPrimitive[T]): (S, Option[T]) => U = (stmt, token) =>
    stmt match {
      case insert: Insert => insert.value(tokenColumnName, 
          cassTokenSerializer.toCType(tokenFactory.createNewToken)).ifNotExists().asInstanceOf[U]
      case update: Update.Assignments => update.and(QueryBuilder.set(tokenColumnName,
          cassTokenSerializer.toCType(tokenFactory.createNewToken))).asInstanceOf[U]
      case update: Update.Where => update.
          onlyIf(QueryBuilder.eq(tokenColumnName, cassTokenSerializer.toCType(token.get.asInstanceOf[T]))).asInstanceOf[U]
  }
  
  protected def deleteColumns: String = valueColumnName
  
  private def enforceTTL(stmt: BuiltStatement): BuiltStatement = ttl match {
    case Some(duration) => stmt match {
      case upt: Update.Assignments => upt.using(QueryBuilder.ttl(duration.inSeconds))
      case upt: Update.Where => upt.using(QueryBuilder.ttl(duration.inSeconds))
      case ins: Insert => ins.using(QueryBuilder.ttl(duration.inSeconds))
      case _ => stmt
    }
    case None => stmt
  }
  
  protected def createPutQuery[K1 <: (RK, CK), T](token: Option[T], casPut: Option[(BuiltStatement, Option[T]) => BuiltStatement])(kv: (K1, Option[V])): BuiltStatement = {
    val ((rk, ck), valueOpt) = kv
    val eqList = new ArrayBuffer[Clause]
    valueOpt match {
      case Some(value) => {
        enforceTTL{
          if(token.isEmpty) {
            val insert = putValue[Insert, Insert, T](value, QueryBuilder.insertInto(columnFamily.getPreparedNamed), token)
            val rowInsertRK = addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer, Some(insert))
            val rowInsertCK = addKey(ck, colkeyColumnNames, eqList, colkeySerializer, rowInsertRK).get
            casPut.map(_(rowInsertCK, token)).getOrElse(rowInsertCK)
          } else {
            val initialUpdatePlain = QueryBuilder.update(columnFamily.getPreparedNamed).`with`()
            val initialUpdateTokened = casPut.map(_(initialUpdatePlain, token)).getOrElse(initialUpdatePlain).asInstanceOf[Update.Assignments]
            val update: Clause => Update.Where = putValue[Update.Assignments, Update.Assignments, T](value, initialUpdateTokened, token).where(_)
            addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer, None)
            addKey(ck, colkeyColumnNames, eqList, colkeySerializer, None)
            val updateWhere = eqList.join(update)((clause, where) => where.and(clause))
            casPut.map(_(updateWhere, token)).getOrElse(updateWhere)
          }
        }
      }
      case None => eqList.join(QueryBuilder.delete(deleteColumns).from(columnFamily.getPreparedNamed).where(_))((clause, where) => where.and(clause))
    }
  }
  
  override def multiPut[K1 <: (RK, CK)](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]] = {
    if(kvs.size > 0) {
      val result = futurePool {
        val mutator = new BatchStatement(batchType)
    	  kvs.foreach(kv => mutator.add(createPutQuery[K1, Boolean](Some(true), None)(kv)))
     	  mutator.setConsistencyLevel(consistency)
    	  // thread-safe: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html
    	  val res = columnFamily.session.getSession.execute(mutator)
      }
      kvs.map{(kv : (K1, Option[V])) => (kv._1, result)}
    } else {
      Map()
    }
  }

  /**
   * creates statement using a recursion on the implicits of type Append2Composite 
   */
  protected def addKey[K <: HList, S <: List[String], KS <: HList](keys: K, colNames: S, clauses: ArrayBuffer[Clause], ser: KS, insert: Option[Insert] = None)
  		(implicit a2c: Append2Composite[ArrayBuffer[Clause], K, KS]): Option[Insert] = append2Composite(clauses)(keys, colNames, ser, insert)
  
  protected def createGetQuery(key: (RK, CK)): Select.Where = {
    val (rk, ck) = key
    val builder = QueryBuilder
      .select()
      .from(columnFamily.getPreparedNamed)
      .where()
   	val eqList = new ArrayBuffer[Clause]
    addKey(rk, rowkeyColumnNames, eqList, rowkeySerializer)
    addKey(ck, colkeyColumnNames, eqList, colkeySerializer)
    eqList.foreach(clause => builder.and(clause))
    builder
  }
  
  override def get(key: (RK, CK)): Future[Option[V]] = {
    futurePool {
      // thread-safe: http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/Session.html
      val result = columnFamily.session.getSession.execute(createGetQuery(key).limit(1).setConsistencyLevel(consistency))
      result.isExhausted() match {
        case false => getValue(result: ResultSet)
        case true => None
      }
    }
  }
  
  override def getKeyValueFromRow(row: Row): ((RK, CK), V) = {
	  val colDefs = row.getColumnDefinitions().asList().toList
	  // find value
	  val value = getRowValue(row)
	  // row key
	  val rk = addKey[RK, RS](row, rowkeySerializer, rowkeyColumnNames)
	  // column key
	  val ck = addKey[CK, CS](row, colkeySerializer, colkeyColumnNames)
	  ((rk, ck), value)
  }

  /**
   * implementing stores return the value
   */
  def getRowValue(row: Row): V
    
  /**
   * the keys are returned using a recursion on the implicits of type Row2Result 
   */
  protected def addKey[K <: HList, S <: HList](row: Row, serializers: S, names: List[String])
  		(implicit r2r: Row2Result.Aux[S, K]): K = {
    row2result(row, serializers, names)
  }

  /**
   * return comma separated list of column names
   */
  override def getColumnNamesString: String = {
    val sb = new StringBuilder
    rowkeyColumnNames.map(quote(sb, _, true))
    colkeyColumnNames.map(quote(sb, _, true))
    quote(sb, valueColumnName)
    sb.toString
  }
  
  override def close(deadline: Time): Future[Unit] = super[AbstractCQLCassandraStore].close(deadline)
}
