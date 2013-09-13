package com.twitter.storehaus.dynamodb

import java.util.{ Map => JMap }

import com.twitter.algebird.Monoid
import com.twitter.bijection.{ Bijection, Codec, Injection }
import com.twitter.bijection.Conversion.asMethod
import com.twitter.conversions.time._
import com.twitter.util.{ Duration, Future }
import com.twitter.storehaus.{ FutureOps, Store, WithPutTtl }
import com.twitter.storehaus.algebra.MergeableStore

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient, AmazonDynamoDB }
import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

/**
 *  @author Ryan Weald
 */

object DynamoStore {

  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String) = {
    val auth = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val client = new AmazonDynamoDBClient(auth)
    new DynamoStore(client, tableName, primaryKeyColumn, valueColumn)
  }

}

class DynamoStore(val client: AmazonDynamoDB, val tableName: String, val primaryKeyColumn: String, val valueColumn: String)
  extends Store[String, AttributeValue]
{

  override def put(kv: (String, Option[AttributeValue])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => {
        //write the new entry to AWS
        val attributes = Map(
          primaryKeyColumn -> key.as[AttributeValue],
          valueColumn -> value
        ).as[JMap[String, AttributeValue]]
        val putRequest = new PutItemRequest(tableName, attributes)

        Future {
          client.putItem(putRequest)
        }

      }

      case (key, None) => {
        val attributes = Map(primaryKeyColumn -> key.as[AttributeValue]).as[JMap[String, AttributeValue]]
        val deleteRequest = new DeleteItemRequest(tableName, attributes)

        Future {
          client.deleteItem(deleteRequest)
        }
      }

    }

  }

  override def get(k: String): Future[Option[AttributeValue]] = {
    val attributes = Map(primaryKeyColumn -> k.as[AttributeValue]).as[JMap[String, AttributeValue]]
    val getRequest = new GetItemRequest(tableName, attributes)

    Future {
      Option(client.getItem(getRequest).getItem) match {
        case Some(response) => {
          Option(response.get(valueColumn))
        }
        case None => None
      }
    }
  }

}

