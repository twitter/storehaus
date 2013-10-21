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

import java.util.{ Map => JMap }

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Conversion.asMethod
import com.twitter.util.Future
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore

import scala.util.Try

import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

object DynamoLongStore {
  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String) =
    new DynamoLongStore(DynamoStore(awsAccessKey, awsSecretKey, tableName, primaryKeyColumn, valueColumn))
}

class DynamoLongStore(underlying: DynamoStore)
  extends ConvertedStore[String, String, AttributeValue, Long](underlying)(identity)
  with MergeableStore[String, Long] {

  def semigroup = implicitly[Semigroup[Long]]

  override def merge(kv: (String, Long)) = {
    val attributeUpdateValue = new AttributeValueUpdate(
      kv._2.as[AttributeValue],
      AttributeAction.ADD
    )

    val updateRequest = new UpdateItemRequest(
      underlying.tableName,
      Map(underlying.primaryKeyColumn -> kv._1.as[AttributeValue]).as[JMap[String, AttributeValue]],
      Map(underlying.valueColumn -> attributeUpdateValue).as[JMap[String, AttributeValueUpdate]]
    )

    underlying.apiRequestFuturePool {
      val res = underlying.client.updateItem(updateRequest)
      // Returns the result, we need the value before
      Option(res.getAttributes.get(kv._1)).map { av => av.as[Try[Long]].get - kv._2 }
    }
  }
}
