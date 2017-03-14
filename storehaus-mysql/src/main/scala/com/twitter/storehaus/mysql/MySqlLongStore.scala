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

package com.twitter.storehaus.mysql

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.storehaus.mysql.compat.Client

/**
  * @author Ruban Monu
  */

/** Factory for [[com.twitter.storehaus.mysql.MySqlLongStore]] instances. */
object MySqlLongStore {

  def apply(underlying: MySqlStore): MySqlLongStore =
    new MySqlLongStore(underlying)(LongMySqlInjection)

  def apply(client: Client, table: String, kCol: String, vCol: String): MySqlLongStore =
    new MySqlLongStore(MySqlStore(client, table, kCol, vCol))(LongMySqlInjection)
}

/** MySQL store for Long values */
class MySqlLongStore(underlying: MySqlStore)(inj: Injection[Long, MySqlValue])
  extends MergeableMySqlStore[Long](underlying)(inj, implicitly[Semigroup[Long]]) {
}

