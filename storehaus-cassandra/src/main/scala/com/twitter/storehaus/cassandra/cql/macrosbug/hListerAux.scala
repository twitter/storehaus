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

trait HListerAux[-T <: Product, Out <: HList] { def apply(t : T) : Out }

trait HListerAuxInstances {
    implicit def tupleHLister1[A] = new HListerAux[Product1[A], A :: HNil] {
      def apply(t: Product1[A]) = t._1 :: HNil
    }
    implicit def tupleHLister2[A, B] = new HListerAux[Product2[A, B], A :: B :: HNil] {
      def apply(t: Product2[A, B]) = t._1 :: t._2 :: HNil
    }
    implicit def tupleHLister3[A, B, C] = new HListerAux[Product3[A, B, C], A :: B :: C :: HNil] {
      def apply(t: Product3[A, B, C]) = t._1 :: t._2 :: t._3 :: HNil
    }
    implicit def tupleHLister4[A, B, C, D] = new HListerAux[Product4[A, B, C, D], A :: B :: C :: D :: HNil] {
      def apply(t : Product4[A, B, C, D]) = t._1 :: t._2 :: t._3 :: t._4 :: HNil
    }
    implicit def tupleHLister5[A, B, C, D, E] = new HListerAux[Product5[A, B, C, D, E], A :: B :: C :: D :: E :: HNil] {
      def apply(t : Product5[A, B, C, D, E]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: HNil
    }
    implicit def tupleHLister6[A, B, C, D, E, F] = new HListerAux[Product6[A, B, C, D, E, F], A :: B :: C :: D :: E :: F :: HNil] {
      def apply(t : Product6[A, B, C, D, E, F]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: HNil
    }
    implicit def tupleHLister7[A, B, C, D, E, F, G] = new HListerAux[Product7[A, B, C, D, E, F, G], A :: B :: C :: D :: E :: F :: G :: HNil] {
      def apply(t : Product7[A, B, C, D, E, F, G]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: HNil
    }
    implicit def tupleHLister8[A, B, C, D, E, F, G, H] = new HListerAux[Product8[A, B, C, D, E, F, G, H], A :: B :: C :: D :: E :: F :: G :: H :: HNil] {
      def apply(t : Product8[A, B, C, D, E, F, G, H]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: HNil
    }
    implicit def tupleHLister9[A, B, C, D, E, F, G, H, I] = new HListerAux[Product9[A, B, C, D, E, F, G, H, I], 
        A :: B :: C :: D :: E :: F :: G :: H :: I :: HNil] {
      def apply(t : Product9[A, B, C, D, E, F, G, H, I]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: HNil
    }
    implicit def tupleHLister10[A, B, C, D, E, F, G, H, I, J] = new HListerAux[Product10[A, B, C, D, E, F, G, H, I, J],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: HNil] {
      def apply(t : Product10[A, B, C, D, E, F, G, H, I, J]) = t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: HNil
    }
    implicit def tupleHLister11[A, B, C, D, E, F, G, H, I, J, K] = new HListerAux[Product11[A, B, C, D, E, F, G, H, I, J, K],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: HNil] {
      def apply(t : Product11[A, B, C, D, E, F, G, H, I, J, K]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: HNil
    }
    implicit def tupleHLister12[A, B, C, D, E, F, G, H, I, J, K, L] = new HListerAux[Product12[A, B, C, D, E, F, G, H, I, J, K, L],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: HNil] {
      def apply(t : Product12[A, B, C, D, E, F, G, H, I, J, K, L]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: HNil
    }
    implicit def tupleHLister13[A, B, C, D, E, F, G, H, I, J, K, L, M] = new HListerAux[Product13[A, B, C, D, E, F, G, H, I, J, K, L, M], 
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: HNil] {
      def apply(t : Product13[A, B, C, D, E, F, G, H, I, J, K, L, M]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: HNil
    }
    implicit def tupleHLister14[A, B, C, D, E, F, G, H, I, J, K, L, M, N] = new HListerAux[Product14[A, B, C, D, E, F, G, H, I, J, K, L, M, N], 
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: HNil] {
      def apply(t : Product14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: HNil
    }
    implicit def tupleHLister15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] = new HListerAux[Product15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O], 
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: HNil] {
      def apply(t : Product15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: HNil
    }
    implicit def tupleHLister16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] = new HListerAux[Product16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: HNil] {
      def apply(t : Product16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: HNil
    }
    implicit def tupleHLister17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q] = new HListerAux[
        Product17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: HNil] {
      def apply(t : Product17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: HNil
    }
    implicit def tupleHLister18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R] = new HListerAux[
        Product18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: HNil] {
      def apply(t : Product18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: 
        t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: HNil
    }
    implicit def tupleHLister19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S] = new HListerAux[
        Product19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O ::P :: Q :: R :: S :: HNil] {
      def apply(t : Product19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: 
        t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: HNil
    }
    implicit def tupleHLister20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T] = new HListerAux[
        Product20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: HNil] {
      def apply(t : Product20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 :: 
        t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: HNil
    }
    implicit def tupleHLister21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U] = new HListerAux[
        Product21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: HNil] {
      def apply(t : Product21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]) = 
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 ::
        t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: t._21 :: HNil
    }
    implicit def tupleHLister22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V] = new HListerAux[
        Product22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V],
        A :: B :: C :: D :: E :: F :: G :: H :: I :: J :: K :: L :: M :: N :: O :: P :: Q :: R :: S :: T :: U :: V :: HNil] {
      def apply(t : Product22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]) =
        t._1 :: t._2 :: t._3 :: t._4 :: t._5 :: t._6 :: t._7 :: t._8 :: t._9 :: t._10 :: t._11 ::
        t._12 :: t._13 :: t._14 :: t._15 :: t._16 :: t._17 :: t._18 :: t._19 :: t._20 :: t._21 :: t._22 :: HNil
    }
}

object HListerAux extends HListerAuxInstances
