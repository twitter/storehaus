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

package com.twitter.storehaus.algebra

import com.twitter.algebird.Monoid
import com.twitter.util.Future
import com.twitter.storehaus.{ AbstractReadableStore, ReadableStore }

trait StatReporter[K, V] {
  def traceGet(request: Future[Option[V]]): Future[Option[V]] = request
  def traceMultiGet[K1 <: K](request: Map[K1, Future[Option[V]]]): Map[K1, Future[Option[V]]] = request
  def getPresent {}
  def getAbsent {}
  def multiGetPresent {}
  def multiGetAbsent {}
  // Wrap a call to put into the store
  def tracePut(request: Future[Unit]): Future[Unit] = request
  // Something was successfully put into the store
  def putSome {}
  // None was successfully put into the store
  def putNone {}
  // Wrap a call to multi put into the store
  def traceMultiPut[K1 <: K](request: Map[K1, Future[Unit]]): Map[K1, Future[Unit]] = request
  // Something was successfull put into the store via a multiPut
  def multiPutSome {}
  // None was successfull put into the store via a multiPut
  def multiPutNone {}
  // Wrap a call to merge with the store
  def traceMerge(request: Future[Option[V]]): Future[Option[V]] = request
  // The merge was a success, we found something to merge with
  def mergeWithSome {}
  // The merge was a success, we found nothing to merge with
  def mergeWithNone {}
  // Wrap the call to multi merge with the store
  def traceMultiMerge[K1 <: K](request: Map[K1, Future[Option[V]]]): Map[K1, Future[Option[V]]] = request
  // The multi-merge was a success, we found something to merge with
  def multiMergeWithSome {}
  // The multi-merge was a success, we found nothing to merge with
  def multiMergeWithNone {}
}