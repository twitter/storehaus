package com.twitter.storehaus

import com.twitter.bijection.Injection


import scala.util.Try
import scalaz._

/**
  * @author Alexey Ponkin
  */
package object postgres {

  import Scalaz._
  type PostgresValueConverter[A] = Injection[A, PostgresValue]
  type DBRequest[A] = Kleisli[Future, Client, A]

  trait OneWayInjection[T] extends Injection[T, PostgresValue] {
    override def apply(t: T): PostgresValue =
      throw IllegalConversionException(
        s"Can`t convert ${t.getClass.getCanonicalName} to PostgresValue")
  }

  implicit val boolInjection= new OneWayInjection[Boolean] {
    override def invert(b: PostgresValue): Try[Boolean] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Boolean])
    }
  }

  implicit val doubleInjection = new OneWayInjection[Double] {
    override def invert(b: PostgresValue): Try[Double] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Double])
    }
  }

  implicit val floatInjection = new OneWayInjection[Float] {
    override def invert(b: PostgresValue): Try[Float] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Float])
    }
  }

  implicit val shortInjection = new OneWayInjection[Short] {
    override def invert(b: PostgresValue): Try[Short] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Short])
    }
  }

  implicit val intInjection = new OneWayInjection[Int] {
    override def invert(b: PostgresValue): Try[Int] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Int])
    }
  }

  implicit val longInjection = new OneWayInjection[Long] {
    override def invert(b: PostgresValue): Try[Long] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Long])
    }
  }

  implicit val stringInjection = new OneWayInjection[String] {
    override def invert(b: PostgresValue): Try[String] = b match {
      case Column(element) => Try(element.value.asInstanceOf[String])
    }
  }

  implicit val byteaInjection = new OneWayInjection[Array[Byte]] {

    override def invert(b: PostgresValue): Try[Array[Byte]] = b match {
      case Column(element) => Try(element.value.asInstanceOf[Array[Byte]])
    }
  }

  def toEscString(a: Any): String = a match {
    case string: String => s"'$string'"
    case array: Array[Byte] => s"E'\\\\x${array.map("%02X" format _).mkString}'"
    case rest => rest.toString
  }

}
