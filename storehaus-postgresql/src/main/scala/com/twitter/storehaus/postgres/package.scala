package com.twitter.storehaus

import roc.postgresql.ElementDecoder
import roc.types.failures.NullDecodedFailure

/**
  * Created by ponkin on 10/24/16.
  */
package object postgres {

  import roc.types.decoders._

  // Read instances

  implicit val boolRead = new Read[Boolean] {
    override def read(v: PostgresValue): Boolean = v match {
      case RocPostgresValue(e) => e.as[Boolean]
    }
  }

  implicit val doubleRead = new Read[Double] {
    override def read(v: PostgresValue): Double = v match {
      case RocPostgresValue(e) => e.as[Double]
    }
  }

  implicit val floatRead = new Read[Float] {
    override def read(v: PostgresValue): Float = v match {
      case RocPostgresValue(e) => e.as[Float]
    }
  }

  implicit val shortRead = new Read[Short] {
    override def read(v: PostgresValue): Short = v match {
      case RocPostgresValue(e) => e.as[Short]
    }
  }

  implicit val intRead = new Read[Int] {
    override def read(v: PostgresValue): Int = v match {
      case RocPostgresValue(e) => e.as[Int]
    }
  }

  implicit val longRead = new Read[Long] {
    override def read(v: PostgresValue): Long = v match {
      case RocPostgresValue(e) => e.as[Long]
    }
  }

  implicit val stringRead = new Read[String] {
    override def read(v: PostgresValue) = v match {
      case RocPostgresValue(e) => e.as[String]
    }
  }

  implicit val byteaRead = new Read[Array[Byte]] {

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

    override def read(v: PostgresValue): Array[Byte] = v match {
      case RocPostgresValue(e) => e.as[Array[Byte]]
    }
  }

  // Show instances
  implicit val boolShow = new Show[Boolean] {
    override def show(value: Boolean): String = value.toString
  }

  implicit val doubleShow = new Show[Double] {
    override def show(value: Double): String = value.toString
  }

  implicit val floatShow = new Show[Float] {
    override def show(value: Float): String = value.toString
  }

  implicit val shortShow = new Show[Short] {
    override def show(value: Short): String = value.toString
  }

  implicit val intShow = new Show[Int] {
    override def show(value: Int): String = value.toString
  }

  implicit val longShow = new Show[Long] {
    override def show(value: Long): String = value.toString
  }

  implicit val stringShow = new Show[String] {
    override def show(value: String) = s"'$value'"
  }

  implicit val byteaShow = new Show[Array[Byte]] {

    // Convert Array[Byte] to HEX representation
    private def toHex(buf: Array[Byte]): String = buf.map("%02X" format _).mkString

    override def show(value: Array[Byte]) =
      s"E'\\\\x${toHex(value)}'"
  }
}
