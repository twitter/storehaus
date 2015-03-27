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

import com.twitter.util.Try
import java.io.{DataOutput, DataInput, ByteArrayOutputStream, ByteArrayInputStream}
import org.apache.hadoop.io.serializer.{Serialization, SerializationFactory, Serializer}
import org.apache.hadoop.mapred.{InputSplit, JobConf}
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory



/**
 * Split which is used to transmit storehaus-keys to the workers.
 * Assumes that appropriate serializers for List[K] are registered at Hadoop.
 * These must already be registered anyways, otherwise we could not have created splits from the
 * keys in the JobConf, so we use the default Hadoop Serialization mechanisms...
 */
class KeyBasedSplit[K](var keys: List[K], val optconf: Option[JobConf], val serclass: Option[Class[K]]) extends InputSplit {
  // this constructor will be used by Hadoop internally
  def this() = {
    this(List(), None, None)
  }

  @transient val logger = LoggerFactory.getLogger(classOf[KeyBasedSplit[K]])
  
  private val SERIALIZATION_KEY = "io.serializations"
  
  override def getLength = keys.size
  override def getLocations = Array()
  /**
   * iterator to iterate over the keys *once*
   */
  def getIterator: Iterator[K] = keys.iterator
  
  override def readFields(in: DataInput) = {
    val serializations = in.readUTF
    val keyClassName = in.readUTF
    val keyClass = getClass().getClassLoader().loadClass(keyClassName).asInstanceOf[Class[K]]
    // build a fake JobConf to insert the serializations key, so we can create a factory
    val conf = new JobConf()
    conf.set(SERIALIZATION_KEY, serializations)
    val serialization = new SerializationFactory(conf)
    val length = in.readInt()
    logger.debug(s"""JobConf-serialization-paramter is: ${serializations}, 
    key-class is: ${keyClass}, reading ${length} keys.""")
    val bytes = new Array[Byte](in.readInt())
    in.readFully(bytes)
    val buffer = new ArrayBuffer[K]
    val deserializer = serialization.getDeserializer(keyClass)
    deserializer.open(new ByteArrayInputStream(bytes))
    for(item <- 1 to length)
      buffer += deserializer.deserialize(null.asInstanceOf[K])
    deserializer.close()
    keys = buffer.toList
  }
  
  override def write(out: DataOutput) = {
    val buffer = new ByteArrayOutputStream
    val serialization = new SerializationFactory(optconf.get)
    out.writeUTF(optconf.get.get(SERIALIZATION_KEY))
    out.writeUTF(serclass.get.getName())
    logger.debug(s"""JobConf-serialization-paramter is: ${optconf.get.get(SERIALIZATION_KEY)}, 
    key-class is: ${serclass.get.getName()}, writing ${keys.size} keys.""")
    val serializerKeyClass = serialization.getSerializer(serclass.get)
    serializerKeyClass.open(buffer)
    keys.foreach(serializerKeyClass.serialize(_))
    serializerKeyClass.close()
    out.writeInt(keys.size)
    out.writeInt(buffer.size())
    out.write(buffer.toByteArray)
  }
}
