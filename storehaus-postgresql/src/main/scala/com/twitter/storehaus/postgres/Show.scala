package com.twitter.storehaus.postgres

/**
  * Type class for serializing scala types to
  * strings that will be inserted in SQL requests to PostgreSQL
  *
  * Created by ponkin on 10/20/16.
  */
trait Show[A] {
  def show(value: A): String
}

object Show {
  def apply[A: Show]: Show[A] = implicitly
}

