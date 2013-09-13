package com.twitter.storehaus.dynamodb

import java.util.{ Map => JMap }

import com.twitter.algebird.Monoid
import com.twitter.bijection.{ Bijection, Codec, Injection }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.conversions.time._
import com.twitter.util.{ Duration, Future }
import com.twitter.storehaus.{ FutureOps, Store, WithPutTtl, ConvertedStore }
import com.twitter.storehaus.algebra.MergeableStore

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient, AmazonDynamoDB }
import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

object DynamoLongStore {
  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String) =
    new DynamoLongStore(DynamoStore(awsAccessKey, awsSecretKey, tableName, primaryKeyColumn, valueColumn))
}

class DynamoLongStore(underlying: DynamoStore)
  extends ConvertedStore[String, String, AttributeValue, Long](underlying)(identity)
  with MergeableStore[String, Long] {

  val monoid = implicitly[Monoid[Long]]

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

    Future {
      underlying.client.updateItem(updateRequest)
    }
  }
}
