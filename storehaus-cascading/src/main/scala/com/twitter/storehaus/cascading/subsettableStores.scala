/*
 * Copyright 2014 Twitter Inc.
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
package com.twitter.storehaus.cascading

import com.twitter.storehaus.ReadableStore

trait SubsettableStore[K, V] extends ReadableStore[K, V] 

trait PartiallyIterableStore[K, V] extends SubsettableStore[K, V]

trait CursoredStore[K, V] extends SubsettableStore[K, V]

trait OffsettableStore[K, V] extends SubsettableStore[K, V]

// maybe we also want to make QueryableStore a SubsettableStore?