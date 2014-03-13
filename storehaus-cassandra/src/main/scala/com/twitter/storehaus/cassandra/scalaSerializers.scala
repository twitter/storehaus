/*
 * Copyright 2014 SEEBURGER AG
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
package com.twitter.storehaus.cassandra

import com.twitter.bijection.{AbstractInjection, Injection}
import java.math.{BigInteger, BigDecimal}
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import me.prettyprint.cassandra.serializers.{AbstractSerializer, DoubleSerializer, FloatSerializer, 
  IntegerSerializer, LongSerializer, StringSerializer, ShortSerializer, BigDecimalSerializer, BigIntegerSerializer,
  BooleanSerializer, CharSerializer, DateSerializer, ObjectSerializer, UUIDSerializer, TimeUUIDSerializer,
  ByteBufferSerializer, BytesArraySerializer}
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.ddl.ComparatorType
import scala.collection.mutable.{Buffer, ArrayBuffer, ArraySeq}
import scala.util.Try


/**
 * type class for types to serialize
 */
trait CassandraSerializable[C] {
  def getSerializer() : Serializer[C]
}

/**
 * an object with a bunch of implicit Cassandra serializers
 */
object ScalaSerializables {
  implicit val cassScalaLongSerializer = new ScalaLongSerializer
  implicit val cassScalaDoubleSerializer = new ScalaDoubleSerializer
  implicit val cassScalaIntSerializer = new ScalaIntSerializer
  implicit val cassScalaFloatSerializer = new ScalaFloatSerializer
  implicit val cassScalaShortSerializer = new ScalaShortSerializer
  implicit val cassScalaStringSerialier = new ScalaStringSerializer
  implicit val cassScalaBigIntegerSerializer = new ScalaBigIntegerSerializer
  implicit val cassScalaBigDecimalSerializer = new ScalaBigDecimalSerializer
  implicit val cassScalaBooleanSerializer = new ScalaBooleanSerializer
  implicit val cassScalaCharSerializer = new ScalaCharSerializer
  implicit val cassScalaDateSerializer = new ScalaDateSerializer
  implicit val cassScalaObjectSerializer = new ScalaSerializableSerializer
  implicit val cassScalaUUIDSerializer = new ScalaUUIDSerializer
  implicit val cassScalaTimeUUIDSerializer = new ScalaTimeUUIDSerializer
  implicit val cassScalaByteBufferSerializer = new ScalaByteBufferSerializer
  implicit val cassScalaBytesArraysSerializer = new ScalaBytesArraySerializer
  val serializerList : Seq[Any => Option[CassandraSerializable[_]]] = ArraySeq(
            buildSerializerChecker[Long](cassScalaLongSerializer),
            buildSerializerChecker[Double](cassScalaDoubleSerializer),
            buildSerializerChecker[Int](cassScalaIntSerializer), 
            buildSerializerChecker[Float](cassScalaFloatSerializer),
            buildSerializerChecker[Short](cassScalaShortSerializer),
            buildSerializerChecker[String](cassScalaStringSerialier),
            buildSerializerChecker[BigInteger](cassScalaBigIntegerSerializer),
            buildSerializerChecker[BigDecimal](cassScalaBigDecimalSerializer),
            buildSerializerChecker[Boolean](cassScalaBooleanSerializer),
            buildSerializerChecker[Character](cassScalaCharSerializer),
            buildSerializerChecker[Date](cassScalaDateSerializer),
            buildSerializerChecker[UUID](cassScalaUUIDSerializer),
            buildSerializerChecker[ByteBuffer](cassScalaByteBufferSerializer),
            buildSerializerChecker[Array[Byte]](cassScalaBytesArraysSerializer),
            buildSerializerChecker[Object](cassScalaObjectSerializer))
   def buildSerializerChecker[T](serializer: CassandraSerializable[T]) : Any => Option[CassandraSerializable[T]] = {
	 value: Any => 
     value match { 
       case _:T => Some(serializer)
       case _ => None
     }
   }
   def getSerializerForEntity(entity: Any, 
       serializers: List[Any => Option[CassandraSerializable[_]]]) :
       Option[CassandraSerializable[_]] = {
     if(serializers.length > 0) { 
       serializers.head(entity) match {
    	 case Some(value) => Some(value)
    	 case None => getSerializerForEntity(entity, serializers.tail)
	   }
     } else {
       None
     }
   }
}

/**
 * needed injections for injective serializers
 */
object CassSerializerInjections {
  implicit val booleanInjection : Injection[Boolean, java.lang.Boolean] = new AbstractInjection[Boolean, java.lang.Boolean] {
    override def apply(bool: Boolean) = Boolean.box(bool)
    override def invert(bool: java.lang.Boolean) = Try(Boolean.unbox(bool))
  }
  implicit val byteArrayInjection : Injection[Array[Byte], Array[java.lang.Byte]] = new AbstractInjection[Array[Byte], Array[java.lang.Byte]] {
    override def apply(arr: Array[Byte]) = arr.map[java.lang.Byte, Array[java.lang.Byte]]((b: Byte) => b)	
    override def invert(arr: Array[java.lang.Byte]) = Try(arr.map[Byte, Array[Byte]]((b: java.lang.Byte) => b))
  }
}
import CassSerializerInjections._

/**
 * some Cassandra-Serializers for some basic Scala types
 */
class InjectiveSerializer[T, U](original: Serializer[U])(implicit injection: Injection[T, U]) 
	extends Serializer[T] with CassandraSerializable[T] {
  import scala.collection.JavaConversions._
  
  override def toByteBuffer(obj: T) : ByteBuffer = 
    original.toByteBuffer(injection.apply(obj))
  override def toBytes(obj: T) : Array[Byte] = 
    original.toBytes(injection.apply(obj))
  override def fromBytes(bytes: Array[Byte]) = 
    injection.invert(original.fromBytes(bytes)).get
  override def fromByteBuffer(byteBuffer: ByteBuffer) = 
    injection.invert(original.fromByteBuffer(byteBuffer)).get
  override def toBytesSet(list: java.util.List[T]) = 
    original.toBytesSet(list.map(injection.apply(_)))
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = 
    original.fromBytesSet(list).map(injection.invert(_).get)
  override def toBytesMap[V](map: java.util.Map[T, V]) = 
    original.toBytesMap(map.map((tv : (T,V)) => (injection.apply(tv._1), tv._2)))
  override def fromBytesMap[V](map: java.util.Map[ByteBuffer, V]) = 
    original.fromBytesMap(map).map((uv : (U,V)) => (injection.invert(uv._1).get, uv._2))
  override def toBytesList(list: java.util.List[T]) = 
    original.toBytesList(list.map(injection.apply(_)))
  override def fromBytesList(list: java.util.List[ByteBuffer]) = 
    original.fromBytesList(list).map(injection.invert(_).get)
  override def getComparatorType = original.getComparatorType
  override def getSerializer = new InjectiveSerializer[T, U](original)
}

object ScalaLongSerializer {
  val serializer = new ScalaLongSerializer
  def apply(ignore : Unit): ScalaLongSerializer = serializer
}
class ScalaLongSerializer(val original: Serializer[java.lang.Long] = LongSerializer.get) 
	extends InjectiveSerializer[Long, java.lang.Long](original)

object ScalaIntSerializer {
  val serializer = new ScalaIntSerializer
  def apply(ignore : Unit): ScalaIntSerializer = serializer
}
class ScalaIntSerializer(val original: Serializer[java.lang.Integer] = IntegerSerializer.get) 
	extends InjectiveSerializer[Int, java.lang.Integer](original)
	
object ScalaDoubleSerializer {
  val serializer = new ScalaDoubleSerializer
  def apply(ignore : Unit): ScalaDoubleSerializer = serializer
}
class ScalaDoubleSerializer(val original: Serializer[java.lang.Double] = DoubleSerializer.get) 
	extends InjectiveSerializer[Double, java.lang.Double](original)

object ScalaFloatSerializer {
  val serializer = new ScalaFloatSerializer
  def apply(ignore : Unit): ScalaFloatSerializer = serializer
}
class ScalaFloatSerializer(val original: Serializer[java.lang.Float] = FloatSerializer.get) 
	extends InjectiveSerializer[Float, java.lang.Float](original)

object ScalaShortSerializer {
  val serializer = new ScalaShortSerializer
  def apply(ignore : Unit): ScalaShortSerializer = serializer
}
class ScalaShortSerializer(val original: Serializer[java.lang.Short] = ShortSerializer.get) 
	extends InjectiveSerializer[Short, java.lang.Short](original)

object ScalaBooleanSerializer {
  val serializer = new ScalaBooleanSerializer
  def apply(ignore : Unit): ScalaBooleanSerializer = serializer
}
class ScalaBooleanSerializer(val original: Serializer[java.lang.Boolean] = BooleanSerializer.get) 
	extends InjectiveSerializer[Boolean, java.lang.Boolean](original)

object ScalaCharSerializer {
  val serializer = new ScalaCharSerializer
  def apply(ignore : Unit): ScalaCharSerializer = serializer
}
class ScalaCharSerializer(val original: Serializer[java.lang.Character] = CharSerializer.get) 
	extends InjectiveSerializer[Character, java.lang.Character](original)

object ScalaBytesArraySerializer {
  val serializer = new ScalaBytesArraySerializer
  def apply(ignore : Unit): ScalaBytesArraySerializer = serializer
}
class ScalaBytesArraySerializer(val original: Serializer[Array[java.lang.Byte]] = 
  BytesArraySerializer.get.asInstanceOf[Serializer[Array[java.lang.Byte]]]) 
	extends InjectiveSerializer[Array[Byte], Array[java.lang.Byte]](original)	
	
/**
 * Cassandra serializer for Scala's Tuple2
 */
object Tuple2Serializer {
  def apply[M : CassandraSerializable, N : CassandraSerializable](ignore: Unit) : Tuple2Serializer[M, N] = {
    new Tuple2Serializer[M, N]
  }
  def get[M : CassandraSerializable, N : CassandraSerializable]() : Tuple2Serializer[M, N] = apply()
}
class Tuple2Serializer[M : CassandraSerializable, N : CassandraSerializable]
	extends AbstractSerializer[(M, N)] with CassandraSerializable[(M, N)] { 
  val child1: CassandraSerializable[M] = implicitly[CassandraSerializable[M]]
  val child2: CassandraSerializable[N] = implicitly[CassandraSerializable[N]]
  override def toByteBuffer(obj: (M, N)) = {
    val byteBuffer1 = child1.getSerializer.toByteBuffer(obj._1)
    val byteBuffer2 = child2.getSerializer.toByteBuffer(obj._2)
    ByteBuffer.allocate(4 + byteBuffer1.remaining() + byteBuffer2.remaining())
    	.putInt(byteBuffer1.remaining())
    	.put(byteBuffer1)
    	.put(byteBuffer2)
    	.flip
    	.asInstanceOf[ByteBuffer]
  }
  override def fromByteBuffer(byteBuffer: ByteBuffer) = {
    val count1 = byteBuffer.getInt()
    val count2 = byteBuffer.remaining() - count1
    val byteArr1 = new Array[Byte](count1)
    val byteArr2 = new Array[Byte](count2)
    byteBuffer.get(byteArr1)
    byteBuffer.get(byteArr2)
    (child1.getSerializer.fromByteBuffer(ByteBuffer.wrap(byteArr1)), child2.getSerializer.fromByteBuffer(ByteBuffer.wrap(byteArr2)))
  }
  override def getSerializer = new Tuple2Serializer[M,N]
}

/**
 * several classes are declared final and we need to make them member of 
 * our type class CassandraSerializable so we need a interim class
 * (can we do as short as this with the pimp-lib-pattern?)
 */
class ScalaSerializer[T](original: Serializer[T]) 
	extends Serializer[T] with CassandraSerializable[T] {
  override def toByteBuffer(obj: T) : ByteBuffer = original.toByteBuffer(obj)
  override def toBytes(obj: T) : Array[Byte] = original.toBytes(obj)
  override def fromBytes(bytes: Array[Byte]) = original.fromBytes(bytes)
  override def fromByteBuffer(byteBuffer: ByteBuffer) = original.fromByteBuffer(byteBuffer)
  override def toBytesSet(list: java.util.List[T]) = original.toBytesSet(list)
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = original.fromBytesSet(list)
  override def toBytesMap[V](map: java.util.Map[T, V]) = original.toBytesMap(map)
  override def fromBytesMap[V](map: java.util.Map[ByteBuffer, V]) = original.fromBytesMap(map)
  override def toBytesList(list: java.util.List[T]) = original.toBytesList(list)
  override def fromBytesList(list: java.util.List[ByteBuffer]) = original.fromBytesList(list)
  override def getComparatorType = original.getComparatorType
  override def getSerializer = new ScalaSerializer[T](original)
}

class ScalaStringSerializer(val original: Serializer[String] = StringSerializer.get) 
	extends ScalaSerializer[String](original: Serializer[String]) 
	
class ScalaBigIntegerSerializer(val original: Serializer[BigInteger] = BigIntegerSerializer.get) 
	extends ScalaSerializer[BigInteger](original: Serializer[BigInteger])
	
class ScalaBigDecimalSerializer(val original: Serializer[BigDecimal] = BigDecimalSerializer.get) 
	extends ScalaSerializer[BigDecimal](original: Serializer[BigDecimal])
	
class ScalaDateSerializer(val original: Serializer[Date] = DateSerializer.get) 
	extends ScalaSerializer[Date](original: Serializer[Date]) 

class ScalaSerializableSerializer(val original: Serializer[Object] = ObjectSerializer.get) 
	extends ScalaSerializer[Object](original: Serializer[Object]) 

class ScalaUUIDSerializer(val original: Serializer[UUID] = UUIDSerializer.get) 
	extends ScalaSerializer[UUID](original: Serializer[UUID]) 

class ScalaTimeUUIDSerializer(val original: Serializer[com.eaio.uuid.UUID] = TimeUUIDSerializer.get)
	extends ScalaSerializer[com.eaio.uuid.UUID](original: Serializer[com.eaio.uuid.UUID])
	
class ScalaByteBufferSerializer(val original: Serializer[ByteBuffer] = ByteBufferSerializer.get)
	extends ScalaSerializer[ByteBuffer](original: Serializer[ByteBuffer])
	
/**
 * composite key definition class with implicit serializers
 */
class ScalaKeyComposite(
  val rowKeys: List[_],
  val rowKeySerializers: List[Serializer[_]],
  val colKeys: List[_],
  val colKeySerializers: List[Serializer[_]]) {
  def this() = this(List(), List(), List(), List())
  def addRow[RK : CassandraSerializable](keyComponent : RK) : ScalaKeyComposite = {
    val ser = implicitly[CassandraSerializable[RK]]
    new ScalaKeyComposite(rowKeys :+ keyComponent, rowKeySerializers :+ ser.getSerializer, colKeys, colKeySerializers)
  }
  def column[CK : CassandraSerializable](keyComponent : CK) : ScalaKeyComposite = {
    val ser = implicitly[CassandraSerializable[CK]]
    new ScalaKeyComposite(rowKeys, rowKeySerializers, colKeys :+ keyComponent, colKeySerializers :+ ser.getSerializer)
  }
  
}