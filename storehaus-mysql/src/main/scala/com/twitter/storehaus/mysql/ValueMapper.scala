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

import com.twitter.bijection.Injection
import com.twitter.finagle.mysql.{
  MysqlCharset,
  EmptyValue,
  IntValue,
  LongValue,
  NullValue,
  RawValue,
  ShortValue,
  StringValue,
  Value,
  Type
}

import scala.util.Try

/** Helper class for mapping finagle-mysql Values to types we care about. */
object ValueMapper {

  // for finagle Value mappings, see:
  // https://github.com/twitter/finagle/blob/master/finagle-mysql/src/main/scala/com/twitter/
  // finagle/mysql/Value.scala

  // currently supported types and corresponding finagle types:
  // INTEGER, INT, MEDIUMINT  => IntValue
  // BIGINT => LongValue
  // SMALLINT => ShortValue
  // BLOB => RawValue
  // TEXT => RawValue
  // CHAR/VARCHAR => StringValue
  def toString(value: Value): Option[String] = {
    value match {
      case IntValue(v) => Some(v.toString)
      case LongValue(v) => Some(v.toString)
      case RawValue(_, _, _, v) => Some(new String(v)) // from byte array
      case ShortValue(v) => Some(v.toString)
      case StringValue(v) => Some(v)
      // finagle-mysql text protocol wraps null strings as NullValue
      // and empty strings as EmptyValue
      case EmptyValue => Some("")
      case NullValue => None
      // all other types are currently unsupported
      case other => throw new UnsupportedOperationException(
        s"${value.getClass.getName} with value $other is currently not supported.")
    }
  }

  def toLong(v: Value): Option[Long] = {
    toString(v).map { _.toLong }
  }
}

/** Factory for [[com.twitter.storehaus.mysql.MySqlValue]] instances. */
object MySqlValue {
  def apply(v: Any): MySqlValue = v match {
    case v: Value => new MySqlValue(v)
    case v: String =>
      new MySqlValue(RawValue(Type.String, MysqlCharset.Utf8_general_ci, isBinary = false, v.getBytes))
    case v: Int => new MySqlValue(IntValue(v))
    case v: Long => new MySqlValue(LongValue(v))
    case v: Short => new MySqlValue(ShortValue(v))
    case other => throw new UnsupportedOperationException(
      s"${v.getClass.getName} with value $other is currently not supported.")
  }
}

/**
 * Wraps finagle-mysql Value ADT.
 *
 * Since finagle maps MySQL column types to specific Value types,
 * we use this type class as an abstraction.
 * MySqlValue objects can then be converted to string or any other type
 * without having to worry about the underlying finagle type.
 */
class MySqlValue(val v: Value) {
  override def equals(o: Any): Boolean = o match {
    // we consider two values to be equal if their underlying string representation are equal
    case o: MySqlValue => ValueMapper.toString(o.v) == ValueMapper.toString(this.v)
    case _ => false
  }
  override def hashCode: Int = {
    ValueMapper.toString(this.v).hashCode
  }
}

/**
 * Injection from MySqlValue to String.
 * Returns string representation of the finagle-mysql Value wrapped by MySqlValue
 * Both null values and empty values map to empty string.
 */
@deprecated("Use String2MySqlValueInjection", "0.10.0")
object MySqlStringInjection extends Injection[MySqlValue, String] {
  // should this be null: String instead?
  def apply(a: MySqlValue): String = ValueMapper.toString(a.v).getOrElse("")
  override def invert(b: String): Try[MySqlValue] =
    Try(MySqlValue(RawValue(Type.String, MysqlCharset.Utf8_general_ci, isBinary = false, b.getBytes)))
}

object String2MySqlValueInjection extends Injection[String, MySqlValue] {
  def apply(s: String): MySqlValue = MySqlValue(s)
  override def invert(m: MySqlValue): Try[String] = Try { ValueMapper.toString(m.v).get }
}

object LongMySqlInjection extends Injection[Long, MySqlValue] {
  def apply(a: Long): MySqlValue = MySqlValue(a)
  override def invert(b: MySqlValue): Try[Long] = Try(ValueMapper.toLong(b.v).get)
}
