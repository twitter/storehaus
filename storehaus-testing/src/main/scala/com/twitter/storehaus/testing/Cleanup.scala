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

import java.io.Closeable
import scala.collection.mutable.{ HashSet, SynchronizedSet }

trait Cleanup {
  Cleanup.instances += this
  def cleanup()
}

trait CloseableCleanup[C <: Closeable] extends Cleanup {
  def closeable: C
  def cleanup() {
    closeable.close()
  }
}

object Cleanup {
  private val instances = new HashSet[Cleanup] with SynchronizedSet[Cleanup]
  def cleanup() {
    try instances.foreach { _.cleanup() }
    catch {
      case e: Exception =>
        println("Error on cleanup")
        e.printStackTrace
    }
  }
}
