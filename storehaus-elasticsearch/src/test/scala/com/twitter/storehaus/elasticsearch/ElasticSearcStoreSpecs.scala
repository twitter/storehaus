/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.twitter.storehaus.elasticsearch

import org.specs2.mutable.Specification
import com.twitter.util.Await
import com.twitter.storehaus.FutureOps

/**
 * @author Muhammad Ashraf
 * @since 1/13/14
 */
class ElasticSearcStoreSpecs extends Specification {


  private val person = Person("Joe", "Smith", 29)

  "ElasticSearch String Store" should {

    "Put a value" in new DefaultElasticContext {
      private val key = "put_key"
      store.put((key, Some(person)))

      blockAndRefreshIndex

      val result = Await.result(store.get(key))
      result === Some(person)
    }

    "Update a value" in new DefaultElasticContext {
      private val key = "update_key"
      store.put(key, Some(person))
      store.put(key, Some(person.copy(age = 30)))

      blockAndRefreshIndex

      val result = Await.result(store.get(key))
      result === Some(person.copy(age = 30))
    }

    "Delete a value" in new DefaultElasticContext {
      private val key = "delete_key"
      store.put(key, Some(person))
      store.put(key, None)

      blockAndRefreshIndex

      val result = Await.result(store.get(key))
      result === None
    }

    "Put multiple values" in new DefaultElasticContext {
      val key = "_put_key"
      val persons = (1 to 10).map(i => i + key -> Some(person.copy(age = i))).toMap

      store.multiPut(persons)

      blockAndRefreshIndex

      val response = store.multiGet(persons.keySet)
      val result = Await.result(FutureOps.mapCollect(response))
      result === persons
    }

    "Update multiple values" in new DefaultElasticContext {
      val key = "_update_key"

      val persons = (1 to 10).map(i => i + key -> Some(person.copy(age = i))).toMap
      val persons_updated = (1 to 10).map(i => i + key -> Some(person.copy(age = i * 2))).toMap

      store.multiPut(persons)
      store.multiPut(persons_updated)
      blockAndRefreshIndex

      val response = store.multiGet(persons_updated.keySet)
      val result = Await.result(FutureOps.mapCollect(response))
      result === persons_updated
    }

    "Delete multiple values" in new DefaultElasticContext {
      val key = "_delete_key"

      val persons = (1 to 10).map(i => i + key -> Some(person.copy(age = i))).toMap
      val deleted_persons = (1 to 10).map(i => i + key -> None).toMap

      store.multiPut(persons)
      store.multiPut(deleted_persons)
      blockAndRefreshIndex

      val response = store.multiGet(deleted_persons.keySet)
      val result = Await.result(FutureOps.mapCollect(response))
      result === deleted_persons
    }
  }

}
