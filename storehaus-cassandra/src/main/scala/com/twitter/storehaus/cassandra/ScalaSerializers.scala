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

import java.nio.ByteBuffer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers.AbstractSerializer
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.DoubleSerializer
import me.prettyprint.cassandra.serializers.FloatSerializer

/**
 * some Cassandra-Serializers for some basic Scala types
 */

object ScalaLongSerializer {
  val serializer = new ScalaLongSerializer
  def apply(ignore : Unit): ScalaLongSerializer = serializer
  def get() : ScalaLongSerializer = apply()
}

class ScalaLongSerializer extends Serializer[Long] {

  import scala.collection.JavaConversions._
  
   /**
   * delegate everything
   */
  override def toByteBuffer(obj: Long) = LongSerializer.get().toByteBuffer(obj)
  override def toBytes(obj: Long) = LongSerializer.get().toBytes(obj)
  override def fromBytes(bytes: Array[Byte]) = LongSerializer.get().fromBytes(bytes)
  override def fromByteBuffer(byteBuffer: ByteBuffer) = LongSerializer.get().fromByteBuffer(byteBuffer)
  override def toBytesSet(list: java.util.List[Long]) = LongSerializer.get().toBytesSet(list.toList.map(java.lang.Long.valueOf(_)))
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = LongSerializer.get().fromBytesSet(list).toList.map(_.longValue)
  override def toBytesMap[V](map : java.util.Map[Long, V]) : java.util.Map[ByteBuffer, V] =  
    LongSerializer.get().toBytesMap(map.toMap.map{(kv :(Long,V)) => (java.lang.Long.valueOf(kv._1), kv._2)})
  override def fromBytesMap[V](map : java.util.Map[ByteBuffer, V]) : java.util.Map[Long, V] = 
    mapAsJavaMap(LongSerializer.get().fromBytesMap(map).map{(kv :(java.lang.Long,V)) => (kv._1, kv._2)}.asInstanceOf[Map[Long, V]])
  override def toBytesList(list: java.util.List[Long]) = LongSerializer.get().toBytesList(list.toList.map(java.lang.Long.valueOf(_)))
  override def fromBytesList(list : java.util.List[ByteBuffer]) = LongSerializer.get().fromBytesList(list).toList.map(_.longValue)
  override def getComparatorType() = LongSerializer.get().getComparatorType()  
}

object ScalaIntSerializer {
  val serializer = new ScalaIntSerializer
  def apply(ignore : Unit): ScalaIntSerializer = serializer
  def get() : ScalaIntSerializer = apply()
}

class ScalaIntSerializer extends Serializer[Int] {

  import scala.collection.JavaConversions._
  
   /**
   * delegate everything
   */
  override def toByteBuffer(obj: Int) = LongSerializer.get().toByteBuffer(obj)
  override def toBytes(obj: Int) = LongSerializer.get().toBytes(obj)
  override def fromBytes(bytes: Array[Byte]) = IntegerSerializer.get().fromBytes(bytes)
  override def fromByteBuffer(byteBuffer: ByteBuffer) = IntegerSerializer.get().fromByteBuffer(byteBuffer)
  override def toBytesSet(list: java.util.List[Int]) = IntegerSerializer.get().toBytesSet(list.toList.map(java.lang.Integer.valueOf(_)))
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = IntegerSerializer.get().fromBytesSet(list).toList.map(_.intValue)
  override def toBytesMap[V](map : java.util.Map[Int, V]) : java.util.Map[ByteBuffer, V] =  
    IntegerSerializer.get().toBytesMap(map.toMap.map{(kv :(Int,V)) => (java.lang.Integer.valueOf(kv._1), kv._2)})
  override def fromBytesMap[V](map : java.util.Map[ByteBuffer, V]) : java.util.Map[Int, V] = 
    mapAsJavaMap(IntegerSerializer.get().fromBytesMap(map).map{(kv :(java.lang.Integer,V)) => (kv._1, kv._2)}.asInstanceOf[Map[Int, V]])
  override def toBytesList(list: java.util.List[Int]) = IntegerSerializer.get().toBytesList(list.toList.map(java.lang.Integer.valueOf(_)))
  override def fromBytesList(list : java.util.List[ByteBuffer]) = IntegerSerializer.get().fromBytesList(list).toList.map(_.intValue)
  override def getComparatorType() = IntegerSerializer.get().getComparatorType()  
}

object ScalaDoubleSerializer {
  val serializer = new ScalaDoubleSerializer
  def apply(ignore : Unit): ScalaDoubleSerializer = serializer
  def get() : ScalaDoubleSerializer = apply()
}

class ScalaDoubleSerializer extends Serializer[Double] {

  import scala.collection.JavaConversions._
  
   /**
   * delegate everything
   */
  override def toByteBuffer(obj: Double) = DoubleSerializer.get().toByteBuffer(obj)
  override def toBytes(obj: Double) = DoubleSerializer.get().toBytes(obj)
  override def fromBytes(bytes: Array[Byte]) = DoubleSerializer.get().fromBytes(bytes)
  override def fromByteBuffer(byteBuffer: ByteBuffer) = DoubleSerializer.get().fromByteBuffer(byteBuffer)
  override def toBytesSet(list: java.util.List[Double]) = DoubleSerializer.get().toBytesSet(list.toList.map(java.lang.Double.valueOf(_)))
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = DoubleSerializer.get().fromBytesSet(list).toList.map(_.doubleValue)
  override def toBytesMap[V](map : java.util.Map[Double, V]) : java.util.Map[ByteBuffer, V] =  
    DoubleSerializer.get().toBytesMap(map.toMap.map{(kv :(Double,V)) => (java.lang.Double.valueOf(kv._1), kv._2)})
  override def fromBytesMap[V](map : java.util.Map[ByteBuffer, V]) : java.util.Map[Double, V] = 
    mapAsJavaMap(DoubleSerializer.get().fromBytesMap(map).map{(kv :(java.lang.Double,V)) => (kv._1, kv._2)}.asInstanceOf[Map[Double, V]])
  override def toBytesList(list: java.util.List[Double]) = DoubleSerializer.get().toBytesList(list.toList.map(java.lang.Double.valueOf(_)))
  override def fromBytesList(list : java.util.List[ByteBuffer]) = DoubleSerializer.get().fromBytesList(list).toList.map(_.doubleValue)
  override def getComparatorType() = DoubleSerializer.get().getComparatorType()  
}

object ScalaFloatSerializer {
  val serializer = new ScalaFloatSerializer
  def apply(ignore : Unit): ScalaFloatSerializer = serializer
  def get() : ScalaFloatSerializer = apply()
}

class ScalaFloatSerializer extends Serializer[Float] {

  import scala.collection.JavaConversions._
  
   /**
   * delegate everything
   */
  override def toByteBuffer(obj: Float) = FloatSerializer.get().toByteBuffer(obj)
  override def toBytes(obj: Float) = FloatSerializer.get().toBytes(obj)
  override def fromBytes(bytes: Array[Byte]) = FloatSerializer.get().fromBytes(bytes)
  override def fromByteBuffer(byteBuffer: ByteBuffer) = FloatSerializer.get().fromByteBuffer(byteBuffer)
  override def toBytesSet(list: java.util.List[Float]) = FloatSerializer.get().toBytesSet(list.toList.map(java.lang.Float.valueOf(_)))
  override def fromBytesSet(list: java.util.Set[ByteBuffer]) = FloatSerializer.get().fromBytesSet(list).toList.map(_.floatValue)
  override def toBytesMap[V](map : java.util.Map[Float, V]) : java.util.Map[ByteBuffer, V] =  
    FloatSerializer.get().toBytesMap(map.toMap.map{(kv :(Float,V)) => (java.lang.Float.valueOf(kv._1), kv._2)})
  override def fromBytesMap[V](map : java.util.Map[ByteBuffer, V]) : java.util.Map[Float, V] = 
    mapAsJavaMap(FloatSerializer.get().fromBytesMap(map).map{(kv :(java.lang.Float,V)) => (kv._1, kv._2)}.asInstanceOf[Map[Float, V]])
  override def toBytesList(list: java.util.List[Float]) = FloatSerializer.get().toBytesList(list.toList.map(java.lang.Float.valueOf(_)))
  override def fromBytesList(list : java.util.List[ByteBuffer]) = FloatSerializer.get().fromBytesList(list).toList.map(_.floatValue)
  override def getComparatorType() = FloatSerializer.get().getComparatorType()  
}


trait ChildHoldingSerializer[T] extends Serializer[T] {
  def getChildrenSerializers : List[Serializer[_ <: Any]]
}

object Tuple2Serializer {
  def apply[M,N](child1: Serializer[M], child2: Serializer[N]): Tuple2Serializer[M, N] = {
    new Tuple2Serializer[M,N](child1, child2)
  }
  def get[M,N](child1: Serializer[M], child2: Serializer[N]) : Tuple2Serializer[M,N] = apply(child1, child2)
}

class Tuple2Serializer[M, N](child1: Serializer[M], child2: Serializer[N]) extends AbstractSerializer[Tuple2[M,N]] with ChildHoldingSerializer[Tuple2[M,N]] { 
  override def getChildrenSerializers : List[Serializer[_ <: Any]] = List(child1, child2) 
  override def toByteBuffer(obj: Tuple2[M,N]) = {
    val byteBuffer1 = child1.toByteBuffer(obj._1)
    val byteBuffer2 = child2.toByteBuffer(obj._2)
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
    (child1.fromByteBuffer(ByteBuffer.wrap(byteArr1)), child2.fromByteBuffer(ByteBuffer.wrap(byteArr2)))
  }
}
