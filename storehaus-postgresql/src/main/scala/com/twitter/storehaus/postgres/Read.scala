package com.twitter.storehaus.postgres


/**
  * Type class for deserializing PostgreSQL native data to scala types
  *
  * Created by ponkin on 10/20/16.
  */
trait Read[A] {
  def read(v: PostgresValue): A
}

object Read {
  def apply[A: Read]: Read[A] = implicitly
}