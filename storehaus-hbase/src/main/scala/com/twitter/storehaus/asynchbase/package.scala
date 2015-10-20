/*
 * Copyright 2013 Twitter inc.
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

package com.twitter.storehaus.asynchbase

import com.twitter.util.{Future, Promise}
import com.stumbleupon.async.{Callback, Deferred}

object `package` {
  private class PromiseCallback[T](promise: Promise[T]) extends Callback[Unit, T] {
    override def call(arg: T): Unit = promise.setValue(arg)
  }

  private class PromiseErrback(promise: Promise[_]) extends Callback[Unit, Exception] {
    override def call(exc: Exception): Unit = promise.setException(exc)
  }

  implicit class RichDeferred[T](val deferred: Deferred[T]) extends AnyVal {
    def future: Future[T] = {
      val p = Promise[T]()
      deferred.addCallback(new PromiseCallback(p))
      deferred.addErrback(new PromiseErrback(p))
      p
    }
  }
}
