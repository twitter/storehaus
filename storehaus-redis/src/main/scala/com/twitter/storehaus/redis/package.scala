package com.twitter.storehaus

import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }
import org.jboss.netty.util.CharsetUtil.UTF_8

package object redis {
  implicit def str2ChannelBuffer(str: String): ChannelBuffer =
    ChannelBuffers.copiedBuffer(str, UTF_8)
}
