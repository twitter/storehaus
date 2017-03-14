/*
 * Copyright 2016 Twitter Inc.
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

package com.twitter.storehaus.mysql

package object compat {

  val Mysql = com.twitter.finagle.exp.Mysql

  type Client = com.twitter.finagle.exp.mysql.Client

  type Result = com.twitter.finagle.exp.mysql.Result

  type Parameter = com.twitter.finagle.exp.mysql.Parameter

  val Parameter = com.twitter.finagle.exp.mysql.Parameter

  val EmptyValue = com.twitter.finagle.exp.mysql.EmptyValue

  val IntValue = com.twitter.finagle.exp.mysql.IntValue

  val LongValue = com.twitter.finagle.exp.mysql.LongValue

  val NullValue = com.twitter.finagle.exp.mysql.NullValue

  val RawValue = com.twitter.finagle.exp.mysql.RawValue

  val ShortValue = com.twitter.finagle.exp.mysql.ShortValue

  val StringValue = com.twitter.finagle.exp.mysql.StringValue

  type Value = com.twitter.finagle.exp.mysql.Value

  val Charset = com.twitter.finagle.exp.mysql.Charset

  val Type = com.twitter.finagle.exp.mysql.Type

}
