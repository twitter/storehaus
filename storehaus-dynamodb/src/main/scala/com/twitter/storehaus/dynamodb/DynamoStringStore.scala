package com.twitter.storehaus.dynamodb

import java.util.{ Map => JMap }

import com.twitter.storehaus.ConvertedStore

import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

object DynamoStringStore {
  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String) =
    new DynamoStringStore(DynamoStore(awsAccessKey, awsSecretKey, tableName, primaryKeyColumn, valueColumn))
}

class DynamoStringStore(underlying: DynamoStore)
  extends ConvertedStore[String, String, AttributeValue, String](underlying)(identity)
