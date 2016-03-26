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

package com.twitter.storehaus.kafka

import java.util.concurrent.{Callable, FutureTask}

import org.scalatest.WordSpec

class JavaFutureToTwitterFutureConverterSpec extends WordSpec {
  private val converter = {
    val c = new JavaFutureToTwitterFutureConverter
    c.start()
    c
  }

  "JavaFutureToTwitterFutureConverter" should {
    "do a proper round trip a java future to a twitter future" in {
      val jFuture = new FutureTask[Int](new Callable[Int] {
        override def call(): Int = 3
      })
      val tFuture = converter(jFuture)
      tFuture.toJavaFuture === jFuture
    }
  }
}
