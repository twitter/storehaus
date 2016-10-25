package com.twitter.storehaus.postgres

import roc.postgresql.Element

/**
  * General trait to represent
  * Postgres column value.
  * NOTE: Actual implementation depends on driver for PostgreSQL
  *
  * Created by ponkin on 10/18/16.
  */
sealed trait PostgresValue
// specific for finagle-roc
case class RocPostgresValue(v: Element) extends PostgresValue
