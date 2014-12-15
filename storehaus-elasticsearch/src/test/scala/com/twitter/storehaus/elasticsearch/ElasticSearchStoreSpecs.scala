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
import com.twitter.util.{Future, Await}
import com.twitter.storehaus.FutureOps
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.json4s.{native, NoTypeHints}

/**
 * @author Mansur Ashraf
 * @since 1/13/14
 */
class ElasticSearchStoreSpecs extends Specification {
  sequential

  private implicit val formats = native.Serialization.formats(NoTypeHints)

  private val person = Person("Joe", "Smith", 29)

  "ElasticSearch Store" should {

    "Put a value" in new DefaultElasticContext {
      private val key = "put_key"
      store.put((key, Some(person)))

      blockAndRefreshIndex

      val result = Await.result(store.get(key))
      result === Some(person)
    }

    "Retrieve a value that doesnt exist" in new DefaultElasticContext {
      private val key = "put_key"
      store.put((key, Some(person)))

      blockAndRefreshIndex

      val result = Await.result(store.get("missing_key"))
      result === None
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

    "Retrieve values that do not exist" in new DefaultElasticContext {
      val key = "_put_key"
      val persons = (1 to 10).map(i => i + key -> Some(person.copy(age = i))).toMap

      store.multiPut(persons)

      blockAndRefreshIndex

      val response = store.multiGet(Set[String]())
      val result = Await.result(FutureOps.mapCollect(response))
      result === Map[String,Future[Option[String]]]()
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

    "Search for  values" in new DefaultElasticContext {

      val bookStore = ElasticSearchCaseClassStore[Book]("books", "programming", client)
      val books = Map(
        "0735619670" -> Some(Book(name = "Code Complete", authors = Array("Steve McConnel"), published = 2004)),
        "020161622X" -> Some(Book(name = "The Pragmatic Programmer", authors = Array("Andy Hunt", "Dave Thomas"), published = 1999)),
        "0131103628" -> Some(Book(name = "C Programming Language", authors = Array("Dennis Ritchie", "Brian Kernighan"), published = 1988)),
        "0262033844" -> Some(Book(name = "Introduction to Algorithms", authors = Array("Thomas Cormen", "Charlie Leiserson", "Ronald Rivest", "Clifford Stein"), published = 2009)),
        "0321356683" -> Some(Book(name = "Effective Java", authors = Array("Josh Bloch"), published = 2008))
      )

      bookStore.multiPut(books)
      blockAndRefreshIndex

      //search for a particular author
      val request1 = new SearchRequestBuilder(client).setQuery(termQuery("authors", "josh")).request()
      val response1 = Await.result(bookStore.queryable.get(request1))
      response1 !== None
      response1.get.head.name === "Effective Java"


      //find all the books published after 2001 where author is not Josh Bloch

      val request2 = new SearchRequestBuilder(client)
        .setQuery(
          boolQuery().mustNot(termQuery("authors", "josh"))
        )
        .setPostFilter(
          rangeFilter("published").gt(2001)
        ).request()

      val response2 = Await.result(bookStore.queryable.get(request2))
      response2 !== None
      response2.get.size === 2
    }
  }

}
