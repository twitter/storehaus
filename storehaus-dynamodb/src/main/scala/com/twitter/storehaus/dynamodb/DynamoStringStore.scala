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

import com.twitter.storehaus.ConvertedStore

import com.amazonaws.regions.{ Region, Regions }
import com.amazonaws.services.dynamodbv2.model._

import AwsBijections._

object DynamoStringStore {
  def apply(awsAccessKey: String, awsSecretKey: String, endpoint: Regions, tableName: String,
    primaryKeyColumn: String, valueColumn: String) =

    new DynamoStringStore(DynamoStore(awsAccessKey, awsSecretKey, endpoint, tableName, primaryKeyColumn, valueColumn))
}

class DynamoStringStore(underlying: DynamoStore)
  extends ConvertedStore[String, String, AttributeValue, String](underlying)(identity)
