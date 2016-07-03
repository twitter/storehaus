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

package com.twitter.storehaus.memcache

import com.twitter.bijection.{ Base64String, Bijection, Codec }
import com.twitter.util.Encoder

/**
 * The HashEncoder provides an extensible way to hash a byte array. Summingbird makes use
 * of the HashEncoder in the MemcacheStore; keys in Memcache are restricted to 256
 * characters, and the HashEncoder allows a way around this limit with very little
 * chance of key collision.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class HashEncoder(hashFunc: String) extends Encoder[Array[Byte], Array[Byte]] {
  def encode(bytes: Array[Byte]): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance(hashFunc)
    md.digest(bytes)
  }
}

object HashEncoder {
  // See this reference for other algorithm names:
  // http://docs.oracle.com/javase/1.4.2/docs/guide/security/CryptoSpec.html#AppA
  val DEFAULT_HASH_FUNC = "SHA-256"

  def apply(hashFunc: String = DEFAULT_HASH_FUNC): HashEncoder = new HashEncoder(hashFunc)

  /**
   * Returns a function that encodes a key to a hashed, base64-encoded
   * Memcache key string given a unique namespace string.
   */
  def keyEncoder[T](namespace: String, hashFunc: String = DEFAULT_HASH_FUNC)
    (implicit inj: Codec[T]): T => String = { key: T =>
    def concat(bytes: Array[Byte]): Array[Byte] = namespace.getBytes ++ bytes
    (inj andThen concat _ andThen HashEncoder() andThen
      Bijection.connect[Array[Byte], Base64String])(key).str
  }
}
