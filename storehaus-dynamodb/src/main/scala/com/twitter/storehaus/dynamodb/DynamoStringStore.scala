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

object DynamoStringStore {
  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String) =
    new DynamoStringStore(DynamoStore(awsAccessKey, awsSecretKey, tableName, primaryKeyColumn, valueColumn))
}

class DynamoStringStore(underlying: DynamoStore)
  extends ConvertedStore[String, String, AttributeValue, String](underlying)(identity)
