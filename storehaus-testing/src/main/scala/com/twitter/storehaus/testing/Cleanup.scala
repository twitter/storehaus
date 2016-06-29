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

package com.twitter.storehaus.testing

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.Exception.allCatch

trait Cleanup {
  Cleanup.instances.put(this, ())
  def cleanup(): Unit
}

/** object resolved dynamically by sbt during test cleanup */
object Cleanup {
  private val instances = new ConcurrentHashMap[Cleanup, Unit]()
  /** Clean up all instances capturing ( and logging ) errors */
  def cleanup() {
    instances.keys.asScala.map(c => allCatch.either(c.cleanup()).left.map((c, _))).foreach(_.fold({
      case (closeable, error) =>
        // scalastyle:off
        println("failed to cleanup %s: %s".format(closeable, error.getMessage))
        // scalastyle:on
    }, identity))
  }
}
