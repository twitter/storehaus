/*
 * Copyright 2013 Twitter Inc.
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

package com.twitter.storehaus.mysql

import java.lang.UnsupportedOperationException

import scala.util.control.Exception.allCatch

import com.twitter.bijection.Injection
import com.twitter.finagle.exp.mysql.{
  EmptyValue,
  IntValue,
  LongValue,
  NullValue,
  RawBinaryValue,
  RawStringValue,
  ShortValue,
  StringValue,
  Value
}

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

/** Helper class for mapping finagle-mysql Values to types we care about. */
object ValueMapper {

  // for finagle Value mappings, see:
  // https://github.com/twitter/finagle/blob/master/finagle-mysql/src/main/scala/com/twitter/finagle/mysql/Value.scala

  // currently supported types and corresponding finagle types:
  // INTEGER, INT, MEDIUMINT  => IntValue
  // BIGINT => LongValue
  // SMALLINT => ShortValue
  // BLOB => RawBinaryValue
  // TEXT => RawStringValue
  // CHAR/VARCHAR => StringValue

  def toChannelBuffer(v: Value): Option[ChannelBuffer] = {
    v match {
      case IntValue(d) => Some(ChannelBuffers.copiedBuffer(d.toString, UTF_8))
      case LongValue(d) => Some(ChannelBuffers.copiedBuffer(d.toString, UTF_8))
      case RawBinaryValue(d) => Some(ChannelBuffers.copiedBuffer(d)) // from byte array
      case RawStringValue(d) => Some(ChannelBuffers.copiedBuffer(d, UTF_8))
      case ShortValue(d) => Some(ChannelBuffers.copiedBuffer(d.toString, UTF_8))
      case StringValue(d) => Some(ChannelBuffers.copiedBuffer(d, UTF_8))
      case EmptyValue => Some(ChannelBuffers.EMPTY_BUFFER)
      case NullValue => None
      // all other types are currently unsupported
      case _ => throw new UnsupportedOperationException(v.getClass.getName + " is currently not supported.")
    }
  }

  def toString(v: Value): Option[String] = {
    v match {
      case IntValue(v) => Some(v.toString)
      case LongValue(v) => Some(v.toString)
      case RawBinaryValue(v) => Some(new String(v)) // from byte array
      case RawStringValue(v) => Some(v)
      case ShortValue(v) => Some(v.toString)
      case StringValue(v) => Some(v)
      case EmptyValue => Some("")
      case NullValue => None
      // all other types are currently unsupported
      case _ => throw new UnsupportedOperationException(v.getClass.getName + " is currently not supported.")
    }
  }
}

object MySqlValue {
  def apply(v: Value) = new MySqlValue(v)
}

class MySqlValue(val v: Value) {
  override def equals(o: Any) = o match {
    // we consider two values to be equal if their underlying string representation are equal
    case o: MySqlValue => ValueMapper.toString(o.v) == ValueMapper.toString(this.v)
    case _ => false
  }
  override def hashCode: Int = {
    ValueMapper.toString(this.v).hashCode
  }
}

object MySqlStringInjection extends Injection[MySqlValue, String] {
  def apply(a: MySqlValue): String = ValueMapper.toString(a.v).getOrElse("")
  def invert(b: String): Option[MySqlValue] = allCatch.opt(MySqlValue(RawStringValue(b)))
}

object MySqlCbInjection extends Injection[MySqlValue, ChannelBuffer] {
  def apply(a: MySqlValue): ChannelBuffer = ValueMapper.toChannelBuffer(a.v).getOrElse(ChannelBuffers.EMPTY_BUFFER)
  def invert(b: ChannelBuffer): Option[MySqlValue] = allCatch.opt(MySqlValue(RawStringValue(b.toString(UTF_8))))
}
