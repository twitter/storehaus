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

import com.twitter.storehaus.FutureOps
import com.twitter.util.{FuturePool, Future, Time}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.bulk.BulkRequest
import com.twitter.concurrent.AsyncMutex
import org.elasticsearch.client.Client
import org.elasticsearch.action.search.SearchRequest
import scala.collection.JavaConverters._

/**
 * @author Mansur Ashraf
 * @since 1/9/14
 */

object ElasticSearchStringStore {

  def apply(index: String,
            tipe: String,
            client: Client) = new ElasticSearchStringStore(index, tipe, client)
}

class ElasticSearchStringStore(private val index: String,
                               private val tipe: String, //tipe -> type since type is a reserved keyword
                               @transient private val client: Client) extends QueryableStore[String, String, SearchRequest] {

  private lazy val futurePool = FuturePool.unboundedPool
  private[this] lazy val mutex = new AsyncMutex

  /** get a single key from the store.
    * Prefer multiGet if you are getting more than one key at a time
    */
  override def get(k: String): Future[Option[String]] = futurePool {
    Option {
      client.prepareGet(index, tipe, k).execute().actionGet().getSourceAsString
    }
  }

  /** Get a set of keys from the store.
    * Important: all keys in the input set are in the resulting map. If the store
    * fails to return a value for a given key, that should be represented by a
    * Future.exception.
    */
  override def multiGet[K1 <: String](ks: Set[K1]): Map[K1, Future[Option[String]]] = {
    val f = futurePool {
      val request = client.prepareMultiGet()
      ks.foreach(request.add(index, tipe, _))
      val response = request.execute().actionGet()

      response.iterator().asScala.map {
        r => r.getResponse.getId -> Option(r.getResponse.getSourceAsString)
      }.toMap
    }
    FutureOps.liftValues(ks, f)
  }


  /** Replace a set of keys at one time */
  override def multiPut[K1 <: String](kvs: Map[K1, Option[String]]): Map[K1, Future[Unit]] = {
    val f = mutex.acquire().flatMap {
      p =>
        futurePool {
          val bulkRequest = new BulkRequest
          kvs.foreach {
            case (k, Some(v)) => bulkRequest.add(new IndexRequest(index, tipe, k).source(v))
            case (k, None) => bulkRequest.add(new DeleteRequest(index, tipe, k))
          }
          client.bulk(bulkRequest).actionGet()
        } ensure p.release()
    }
    kvs.mapValues[Future[Unit]](v => f.unit)
  }

  /**
   * replace a value
   * Delete is the same as put((k,None))
   */
  override def put(kv: (String, Option[String])): Future[Unit] = mutex.acquire().flatMap {
    p => futurePool {
      kv match {
        case (k, Some(v)) => client.prepareIndex(index, tipe, k).setSource(v).execute().actionGet()
        case (k, None) => client.prepareDelete(index, tipe, k).execute().actionGet()
      }
    } ensure {
      p.release
    }
      Future.Unit
  }

  /** Close this store and release any resources.
    * It is undefined what happens on get/multiGet after close
    */
  override def close(time: Time): Future[Unit] = mutex.acquire().flatMap {
    p => futurePool {
      client.close()
    } ensure p.release()
  }

  /**
   * Given Query Q return all the values that match that query
   * @param query
   * @return Optional list of values
   */
  def query(query: SearchRequest): Future[Option[List[String]]] = futurePool {
    //Force to use the index and Type this store is configured for.
    val updatedQuery = query.indices(Array(index): _*).types(Array(tipe): _*)
    val searchHits = client.search(updatedQuery).actionGet().getHits
    searchHits.totalHits() match {
      case 0 => None
      case _ => Some(searchHits.hits().toList.map(_.getSourceAsString))
    }
  }
}
