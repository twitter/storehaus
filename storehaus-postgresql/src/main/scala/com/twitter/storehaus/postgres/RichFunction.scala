package com.twitter.storehaus.postgres

/**
  * Implements map, flatMap for functions
  *
  * @author Alexey Ponkin
  */
sealed trait RichFunction1[-T, +R] {
  def apply(t: T): R

  import RichFunction1.rich

  def map[X](g: R => X) = rich[T, X](g compose (apply(_)))

  def flatMap[TT <: T, X](g: R => RichFunction1[TT, X]) =
    rich[TT, X](t => g(apply(t))(t))

}

object RichFunction1 {
  implicit def rich[T, R](f: T => R):  RichFunction1[T, R] = new RichFunction1[T, R] {
    def apply(t: T) = f(t)
  }
}
