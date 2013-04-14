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

import com.twitter.finagle.exp.mysql.{ NullValue, RawBinaryValue, RawStringValue, StringValue, Value }

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil.UTF_8

/** Helper class for mapping finagle-mysql Values to types we care about. */
object ValueMapper {

  // for finagle Value mappings, see:
  // https://github.com/twitter/finagle/blob/master/finagle-mysql/src/main/scala/com/twitter/finagle/mysql/Value.scala

  // currently supported types:
  // BLOB => RawBinaryValue
  // TEXT => RawStringValue
  // CHAR/VARCHAR => StringValue

  def toChannelBuffer(optV: Option[Value]): Option[ChannelBuffer] = {
    optV match {
      case None => None
      case Some(v) => v match {
        case RawBinaryValue(d) => Some(ChannelBuffers.copiedBuffer(d)) // from byte array
        case RawStringValue(d) => Some(ChannelBuffers.copiedBuffer(d, UTF_8))
        case StringValue(d) => Some(ChannelBuffers.copiedBuffer(d, UTF_8))
        case NullValue => None
        // all other types are currently unsupported
        case _ => throw new UnsupportedOperationException(v.getClass.getName + " is currently not supported.")
      }
    }
  }

  def toString(optV: Option[Value]): Option[String] = {
    optV match {
      case None => None
      case Some(v) => v match {
        case RawBinaryValue(v) => Some(new String(v)) // from byte array
        case RawStringValue(v) => Some(v)
        case StringValue(v) => Some(v)
        case NullValue => None
        // all other types are currently unsupported
        case _ => throw new UnsupportedOperationException(v.getClass.getName + " is currently not supported.")
      }
    }
  }
}
