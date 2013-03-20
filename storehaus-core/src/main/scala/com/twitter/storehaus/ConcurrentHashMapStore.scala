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

package com.twitter.storehaus

import java.util.concurrent.{ ConcurrentHashMap => JConcurrentHashMap }

/** A simple JMapStore whose underlying store is a Java ConcurrentHashMap
 * useful if you are going to be modifying a store from many threads, something
 * you in general cannot do unless the docs specifically say that a store is
 * safe for that.
 *
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */
class ConcurrentHashMapStore[K, V] extends JMapStore[K, V] {
  protected override val jstore = new JConcurrentHashMap[K, Option[V]]
}
