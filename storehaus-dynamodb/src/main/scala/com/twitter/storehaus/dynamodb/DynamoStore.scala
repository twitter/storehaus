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
import java.util.concurrent.Executors

import com.twitter.bijection.Conversion.asMethod
import com.twitter.util.{ Future, FuturePool }
import com.twitter.storehaus.Store

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient, AmazonDynamoDB }
import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

/**
 *  @author Ryan Weald
 */

object DynamoStore {

  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String, primaryKeyColumn: String, valueColumn: String): DynamoStore = {
    val processors = Runtime.getRuntime.availableProcessors
    this(awsAccessKey, awsSecretKey, tableName, primaryKeyColumn, valueColumn, processors)
  }

  def apply(awsAccessKey: String, awsSecretKey: String, tableName: String,
    primaryKeyColumn: String, valueColumn: String, numberWorkerThreads: Int): DynamoStore = {

    val auth = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val client = new AmazonDynamoDBClient(auth)
    new DynamoStore(client, tableName, primaryKeyColumn, valueColumn, numberWorkerThreads)
  }

}

class DynamoStore(val client: AmazonDynamoDB, val tableName: String,
  val primaryKeyColumn: String, val valueColumn: String, numberWorkerThreads: Int)
  extends Store[String, AttributeValue]
{

  val apiRequestFuturePool = FuturePool(Executors.newFixedThreadPool(numberWorkerThreads))

  override def put(kv: (String, Option[AttributeValue])): Future[Unit] = {
    kv match {
      case (key, Some(value)) => {
        //write the new entry to AWS
        val attributes = Map(
          primaryKeyColumn -> key.as[AttributeValue],
          valueColumn -> value
        ).as[JMap[String, AttributeValue]]
        val putRequest = new PutItemRequest(tableName, attributes)

        apiRequestFuturePool(client.putItem(putRequest))
      }

      case (key, None) => {
        val attributes = Map(primaryKeyColumn -> key.as[AttributeValue]).as[JMap[String, AttributeValue]]
        val deleteRequest = new DeleteItemRequest(tableName, attributes)

        apiRequestFuturePool(client.deleteItem(deleteRequest))
      }

    }

  }

  override def get(k: String): Future[Option[AttributeValue]] = {
    val attributes = Map(primaryKeyColumn -> k.as[AttributeValue]).as[JMap[String, AttributeValue]]
    val getRequest = new GetItemRequest(tableName, attributes)

    apiRequestFuturePool {
      Option(client.getItem(getRequest).getItem) match {
        case Some(response) => {
          Option(response.get(valueColumn))
        }
        case None => None
      }
    }
  }

}

