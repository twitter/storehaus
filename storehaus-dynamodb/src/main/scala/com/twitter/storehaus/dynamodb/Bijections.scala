package com.twitter.storehaus.dynamodb

import scala.util.Try

import com.twitter.bijection.{ Bijection, Injection }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.{ AbstractBijection, AbstractInjection }
import com.twitter.bijection.NumericInjections
import com.amazonaws.services.dynamodbv2.model.AttributeValue

//TODO: pull these out into the Bijections project
object AwsBijections extends NumericInjections {
  implicit val stringToAttributeValue: Bijection[String, AttributeValue] = new AbstractBijection[String, AttributeValue] {
      override def apply(s: String) = new AttributeValue(s)
      override def invert(a: AttributeValue) = a.getS
  }

  implicit val longToAttributeValue : Injection[Long, AttributeValue] = new AbstractInjection[Long, AttributeValue] {
    override def apply(l: Long) = (new AttributeValue).withN(l.as[String])
    override def invert(a: AttributeValue) = Try(a.getN.toLong)
  }
}
