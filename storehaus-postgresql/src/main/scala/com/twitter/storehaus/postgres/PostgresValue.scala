package com.twitter.storehaus.postgres

import com.twitter.finagle.postgres.values.Value


/**
  * General trait to represent
  * Postgres column value.
  * NOTE: Actual implementation depends on driver for PostgreSQL
  *
  * @author Alexey Ponkin
  */
sealed trait PostgresValue
case class Column(v: Value[Any]) extends PostgresValue
