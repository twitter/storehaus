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

import java.util

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.scalacheck.{Gen, Arbitrary, Properties}
import org.scalacheck.Prop.forAll

object MySqlValueInjectionProperties extends Properties("MySqlValue Injections") {

  val channelBufferGenerator = for {
    b <- Gen.containerOf[Array, Byte](Arbitrary.arbitrary[Byte])
  } yield ChannelBuffers.wrappedBuffer(b)

  implicit val gen = Arbitrary(channelBufferGenerator)

  property("String2MySqlValue apply and invert") = {
    val injection = String2MySqlValueInjection
    forAll { (s: String) =>
      injection.invert(injection(s)).toOption == Some(s)
    }
  }

  property("ChannelBuffer2MySqlValue apply and invert") = {
    val injection = ChannelBuffer2MySqlValueInjection

    forAll { (c: ChannelBuffer) =>
      val bytes = bytesFromChannelBuffer(c)
      val inverted = injection.invert(injection(c)).toOption
      inverted.exists { invertedBuf =>
        val invertedBytes = bytesFromChannelBuffer(invertedBuf)
        util.Arrays.equals(invertedBytes, bytes)
      }
    }
  }

  def bytesFromChannelBuffer(c: ChannelBuffer): Array[Byte] = {
    val arr = Array.ofDim[Byte](c.readableBytes)
    c.markReaderIndex()
    c.readBytes(arr)
    c.resetReaderIndex()
    arr
  }
}
