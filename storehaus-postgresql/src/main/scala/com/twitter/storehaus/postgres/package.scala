package com.twitter.storehaus

import com.twitter.bijection.Injection
import roc.postgresql.ElementDecoder
import roc.types.failures.NullDecodedFailure


import scala.util.Try

/**
  * Created by ponkin on 10/24/16.
  */
package object postgres {

  type PostgresValueConverter[A] = Injection[A, PostgresValue]

  import roc.types.decoders._

  trait OneWayInjection[T] extends Injection[T, PostgresValue] {
    override def apply(t: T): PostgresValue =
      throw IllegalConversionException(s"Can`t convert ${t.getClass.getCanonicalName} to PostgresValue")
  }

  implicit val boolInjection= new OneWayInjection[Boolean] {
    override def invert(b: PostgresValue): Try[Boolean] = b match {
      case RocPostgresValue(element) => Try(element.as[Boolean])
    }
  }

  implicit val doubleInjection = new OneWayInjection[Double] {
    override def invert(b: PostgresValue): Try[Double] = b match {
      case RocPostgresValue(element) => Try(element.as[Double])
    }
  }

  implicit val floatInjection = new OneWayInjection[Float] {
    override def invert(b: PostgresValue): Try[Float] = b match {
      case RocPostgresValue(element) => Try(element.as[Float])
    }
  }

  implicit val shortInjection = new OneWayInjection[Short] {
    override def invert(b: PostgresValue): Try[Short] = b match {
      case RocPostgresValue(element) => Try(element.as[Short])
    }
  }

  implicit val intInjection = new OneWayInjection[Int] {
    override def invert(b: PostgresValue): Try[Int] = b match {
      case RocPostgresValue(element) => Try(element.as[Int])
    }
  }

  implicit val longInjection = new OneWayInjection[Long] {
    override def invert(b: PostgresValue): Try[Long] = b match {
      case RocPostgresValue(element) => Try(element.as[Long])
    }
  }

  implicit val stringInjection = new OneWayInjection[String] {
    override def invert(b: PostgresValue): Try[String] = b match {
      case RocPostgresValue(element) => Try(element.as[String])
    }
  }

  implicit val byteaInjection = new OneWayInjection[Array[Byte]] {

    implicit val byteaElementDecoders: ElementDecoder[Array[Byte]] = new ElementDecoder[Array[Byte]] {
      // ROC returns bytea as string in format \x6fFF
      // where '\x' - bytea type identifier
      // 6f,FF - HEX representation of Byte
      def textDecoder(text: String): Array[Byte] =
      text.toArray.sliding(2, 2).filterNot(_.contains('x')).map { bb =>
        Integer.parseInt(bb.mkString, 16).toByte
      }.toArray

      def binaryDecoder(bytes: Array[Byte]): Array[Byte] = bytes

      def nullDecoder: Array[Byte] = throw new NullDecodedFailure("BYTEA")
    }

    override def invert(b: PostgresValue): Try[Array[Byte]] = b match {
      case RocPostgresValue(element) => Try(element.as[Array[Byte]])
    }
  }

  def toEscString(a: Any): String = a match {
    case string: String => s"'$string'"
    case array: Array[Byte] => s"E'\\\\x${array.map("%02X" format _).mkString}'"
    case rest => rest.toString
  }

}
