/*
 * Copyright 2014 Twitter Inc.
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
package com.twitter.storehaus.cascading.split

import com.twitter.util.{ Try, Await }
import java.io.{ DataOutput, DataInput, ByteArrayOutputStream, ByteArrayInputStream }
import org.apache.hadoop.io.{ DefaultStringifier }
import org.apache.hadoop.io.serializer.{ Serialization, SerializationFactory, Serializer} 
import org.apache.hadoop.mapred.{ InputSplit, JobConf }
import scala.collection.mutable.ArrayBuffer

/**
 * Split which is used to transmit storehaus-keys to the workers.
 * Assumes that appropriate serializers for List[K] are registered
 * at Hadoop
 */
class KeyBasedSplit[K](var keys: List[K], @transient conf: JobConf) extends InputSplit {
  @transient val serializationClass = KeyBasedSplit.getKeySerializerClass(conf).getOrElse(classOf[Serializable]).asInstanceOf[Class[K]]
  @transient val serialization = (new SerializationFactory(conf)).getSerialization(serializationClass).asInstanceOf[Serialization[K]]
  
  val iterator = keys.iterator
  
  override def getLength = keys.size
  override def getLocations = Array()
  /**
   * iterator to iterate over the keys *once*
   */
  def getIterator: Iterator[K] = iterator
  
  override def readFields(in: DataInput) = {
    val length = in.readInt()
    val bytes = new Array[Byte](in.readInt())
    in.readFully(bytes)
    val buffer = new ArrayBuffer[K]
    val deserializer = serialization.getDeserializer(serializationClass)
    deserializer.open(new ByteArrayInputStream(bytes))
    for(item <- 1 to length)
      buffer += deserializer.deserialize(null.asInstanceOf[K])
    deserializer.close()
  }
  override def write(out: DataOutput) = {
    val buffer = new ByteArrayOutputStream
    val serializer = serialization.getSerializer(serializationClass)
    serializer.open(buffer)
    // serializer.
    keys.foreach(x => {
      serializer.serialize(x)
    })
    serializer.close()
    out.write(keys.size)
    out.write(buffer.size())
    out.write(buffer.toByteArray)
  }
}

object KeyBasedSplit {
  val SPLIT_SERIALIZER_CLASS_NAME = "com.twitter.storehaus.cascading.keyserializer.class"
  def getKeySerializerClass[K](conf: JobConf): Try[Class[K]] = {
    Try {
        Class.forName(conf.get(SPLIT_SERIALIZER_CLASS_NAME)).asInstanceOf[Class[K]]
    }    
  }
  /**
   * name of the typeof key that we need to search a serializer for 
   */
  def setKeySerializerClass[K](conf: JobConf, clazz: Class[KeyBasedSplit[_]]) = {
    conf.set(SPLIT_SERIALIZER_CLASS_NAME, clazz.getName())
  }
}

