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

import com.twitter.util.Future
import com.twitter.bijection.{Bijection, AbstractBijection}

import UtilBijections._
import Bijection.asMethod

class BijectedReadableStore[K1,V1,K2,V2](store: ReadableStore[K1,V1])(implicit bik: Bijection[K1,K2], biv: Bijection[V1,V2]) extends AbstractReadableStore[K2,V2] {
  // Somehow we overlooked this. TODO remove when it is added to Bijection
  protected implicit val optionBij: Bijection[Option[V1], Option[V2]] =
    new AbstractBijection[Option[V1], Option[V2]] {
      override def apply(opt1: Option[V1]) = opt1.map(biv)
      override def invert(opt2: Option[V2]) = opt2.map(biv.inverse)
    }

  override def get(k: K2) = store.get(k.as[K1]).as[Future[Option[V2]]]

  override def multiGet(ks: Set[K2]) =
    store.multiGet(ks.as[Set[K1]]).as[Future[Map[K2, Future[Option[V2]]]]]
}

class BijectedStore[Contained <: Store[Contained,K1,V1],K1,V1,K2,V2](val contained: Contained)(implicit bik: Bijection[K1,K2], biv: Bijection[V1,V2]) extends Store[BijectedStore[Contained,K1,V1,K2,V2],K2,V2] {

  // Somehow we overlooked this. TODO remove when it is added to Bijection
  protected implicit val optionBij: Bijection[Option[V1], Option[V2]] =
    new AbstractBijection[Option[V1], Option[V2]] {
      override def apply(opt1: Option[V1]) = opt1.map(biv)
      override def invert(opt2: Option[V2]) = opt2.map(biv.inverse)
    }

  override def get(k: K2) = contained.get(k.as[K1]).as[Future[Option[V2]]]

  override def multiGet(ks: Set[K2]) =
    contained.multiGet(ks.as[Set[K1]]).as[Future[Map[K2, Future[Option[V2]]]]]

  override def -(k: K2) =
    contained.-(k.as[K1]).map { new BijectedStore[Contained,K1,V1,K2,V2](_) }

  override def +(pair: (K2,V2)) =
    contained.+(pair.as[(K1,V1)]).map { new BijectedStore[Contained,K1,V1,K2,V2](_) }

  override def update(k: K2)(fn: Option[V2] => Option[V2]) =
    contained.update(k.as[K1])(fn.as[Option[V1] => Option[V1]])
      .map { new BijectedStore[Contained,K1,V1,K2,V2](_) }
}

