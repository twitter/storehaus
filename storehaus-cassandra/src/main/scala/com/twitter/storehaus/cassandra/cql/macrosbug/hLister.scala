/*
 * Copyright 2014 Twitter, Inc.; some copied from shapeless, also being ASLv2
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
package com.twitter.storehaus.cassandra.cql.macrobug

import shapeless._

trait HLister[-T <: Product] { type Out <: HList; def apply(t : T) : Out }

object HLister {
  implicit def hlister[T <: Product, Out0 <: HList](implicit hlister : HListerAux[T, Out0]) = new HLister[T] {
    type Out = Out0
    def apply(t : T) : Out = hlister(t)
  }
}

