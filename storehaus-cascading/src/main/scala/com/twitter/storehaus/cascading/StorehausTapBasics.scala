/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.storehaus.cascading

import cascading.tap.Tap
import org.apache.hadoop.mapred.{JobConf, RecordReader, OutputCollector}

trait StorehausTapBasics {
  def getModifiedTime(conf: JobConf) : Long = System.currentTimeMillis
  def createResource(conf: JobConf): Boolean = true
  def deleteResource(conf: JobConf): Boolean = true
  def resourceExists(conf: JobConf): Boolean = true
  
  type StorehausTapTypeInJava = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]
}