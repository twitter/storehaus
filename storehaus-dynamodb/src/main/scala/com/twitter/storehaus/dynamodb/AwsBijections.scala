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
package com.twitter.storehaus.dynamodb

import scala.util.Try

import com.twitter.bijection.{ Bijection, Injection }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.{ AbstractBijection, AbstractInjection }
import com.twitter.bijection.NumericInjections
import com.amazonaws.services.dynamodbv2.model.AttributeValue

// TODO: pull these out into the Bijections project
object AwsBijections extends NumericInjections {
  implicit val stringToAttributeValue: Bijection[String, AttributeValue] =
    new AbstractBijection[String, AttributeValue] {
      override def apply(s: String) = new AttributeValue(s)
      override def invert(a: AttributeValue) = a.getS
    }

  implicit val longToAttributeValue : Injection[Long, AttributeValue] =
    new AbstractInjection[Long, AttributeValue] {
      override def apply(l: Long) = (new AttributeValue).withN(l.as[String])
      override def invert(a: AttributeValue) = Try(a.getN.toLong)
    }
}
