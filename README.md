## Storehaus

[![Build Status](https://secure.travis-ci.org/twitter/storehaus.png)](http://travis-ci.org/twitter/storehaus)
[![Codecov branch](https://img.shields.io/codecov/c/github/twitter/storehaus/develop.svg?maxAge=3600)](https://codecov.io/github/twitter/storehaus)
[![Latest version](https://index.scala-lang.org/twitter/storehaus/storehaus-core/latest.svg?color=orange)](https://index.scala-lang.org/twitter/storehaus/storehaus-core)
[![Chat](https://badges.gitter.im/twitter/storehaus.svg)](https://gitter.im/twitter/storehaus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Storehaus is a library that makes it easy to work with asynchronous key value stores. Storehaus is built on top of Twitter's [Future](https://github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/util/Future.scala).

### Storehaus-Core

Storehaus's core module defines three traits; a read-only `ReadableStore` a write-only `WritableStore` and a read-write `Store`. The traits themselves are tiny:

```scala
package com.twitter.storehaus

import com.twitter.util.{ Closable, Future, Time }

trait ReadableStore[-K, +V] extends Closeable {
  def get(k: K): Future[Option[V]]
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]]
  override def close(time: Time) = Future.Unit
}

trait WritableStore[-K, -V] {
  def put(kv: (K, V)): Future[Unit] = multiPut(Map(kv)).apply(kv._1)
  def multiPut[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] =
    kvs.map { kv => (kv._1, put(kv)) }
  override def close(time: Time) = Future.Unit
}

trait Store[-K, V] extends ReadableStore[K, V] with WritableStore[K, Option[V]]
```

The `ReadableStore` trait uses the `Future[Option[V]]` return type to communicate one of three states about each value. A value is either

* definitely present,
* definitely missing, or
* unknown due to some error (perhaps a timeout, or a downed host).

The [`ReadableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.ReadableStore$) and [`Store`](http://twitter.github.com/storehaus/#com.twitter.storehaus.Store$) companion objects provide a bunch of ways to create new stores. See the linked API documentation for more information.

### Combinators

Coding with Storehaus's interfaces gives you access to a number of powerful combinators. The easiest way to access these combinators is by wrapping your store in an [`EnrichedReadableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.EnrichedReadableStore) or an [`EnrichedStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.EnrichedStore). Storehaus provides implicit conversions inside of the [`ReadableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.ReadableStore$) and [`Store`](http://twitter.github.com/storehaus/#com.twitter.storehaus.Store$) objects.

Here's an example of the `mapValues` combinator, useful for transforming the type of an existing store.

```scala
import com.twitter.storehaus.ReadableStore
import ReadableStore.enrich

// Create a ReadableStore from Int -> String:
val store = ReadableStore.fromMap(Map[Int, String](1 -> "some value", 2 -> "other value"))

// "get" behaves as expected:
store.get(1).get
// res5: Option[String] = Some(some value)

// calling "mapValues" with a function from V => NewV returns a new ReadableStore[K, NewV]:
val countStore: ReadableStore[Int, Int] = store.mapValues { s => s.size }

// This new store applies the function to every value on the way out:
countStore.get(1).get
// res6: Option[Int] = Some(10)
```

### Storehaus-Algebra

`storehaus-algebra` module adds the `MergeableStore` trait. If you're using key-value stores for aggregations, you're going to love `MergeableStore`.

```scala
package com.twitter.storehaus.algebra

trait MergeableStore[-K, V] extends Store[K, V] {
  def monoid: Monoid[V]
  def merge(kv: (K, V)): Future[Option[V]] = multiMerge(Map(kv)).apply(kv._1)
  def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Option[V]]] = kvs.map { kv => (kv._1, merge(kv)) }
}
```

`MergeableStore`'s `merge` and `multiMerge` are similar to `put` and `multiPut`; the difference is that values added with `merge` are added to the store's existing value and the previous value is returned.
Because the addition is handled with a `Semigroup[V]` or `Monoid[V]` from Twitter's [Algebird](https://github.com/twitter/algebird) project, it's easy to write stores that aggregate [Lists](http://twitter.github.com/algebird/#com.twitter.algebird.ListMonoid), [decayed values](http://twitter.github.com/algebird/#com.twitter.algebird.DecayedValue), even [HyperLogLog](http://twitter.github.com/algebird/#com.twitter.algebird.HyperLogLog$) instances.

The [`MergeableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.algebra.MergeableStore$) object provides a number of combinators on these stores. For ease of use, Storehaus provides an implicit conversion to an enrichment on `MergeableStore`. Access this by importing `MergeableStore.enrich`.

### Other Modules

Storehaus provides a number of modules wrapping existing key-value stores. Enriching these key-value stores with Storehaus's combinators has been hugely helpful to us here at Twitter. Writing your jobs in terms of Storehaus stores makes it easy to test your jobs; use an in-memory `JMapStore` in testing and a `MemcacheStore` in production.

  * [Storehaus-memcache](http://twitter.github.com/storehaus/#com.twitter.storehaus.memcache.MemcacheStore) (wraps Twitter's [finagle-memcachedx](https://github.com/twitter/finagle/tree/master/finagle-memcachedx) library)
  * [Storehaus-mysql](http://twitter.github.com/storehaus/#com.twitter.storehaus.mysql.MySqlStore) (wraps Twitter's [finagle-mysql](https://github.com/twitter/finagle/tree/master/finagle-mysql) library)
  * [Storehaus-redis](http://twitter.github.com/storehaus/#com.twitter.storehaus.redis.RedisStore) (wraps Twitter's [finagle-redis](https://github.com/twitter/finagle/tree/master/finagle-redis) library)
  * [Storehaus-hbase](http://twitter.github.com/storehaus/#com.twitter.storehaus.hbase.HBaseStore)
  * [Storehaus-dynamodb](https://github.com/twitter/storehaus/tree/develop/storehaus-dynamodb)
  * [Storehaus-leveldb](https://github.com/twitter/storehaus/tree/develop/storehaus-leveldb)

#### Planned Modules

Here's a list of modules we plan in implementing, with links to the github issues tracking progress on these modules:

* [storehaus-berkeleydb](https://github.com/twitter/storehaus/issues/52)

## Documentation

To learn more and find links to tutorials and information around the web, check out the [Storehaus Wiki](https://github.com/twitter/storehaus/wiki).

The latest ScalaDocs are hosted on Storehaus's [Github Project Page](http://twitter.github.io/storehaus).

## Contact

Discussion occurs primarily on the [Storehaus mailing list](https://groups.google.com/forum/#!forum/storehaus). Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/storehaus/issues).

## Get Involved + Code of Conduct
Pull requests and bug reports are always welcome!

We use a lightweight form of project governence inspired by the one used by Apache projects.
Please see [Contributing and Committership](https://github.com/twitter/analytics-infra-governance#contributing-and-committership) for our code of conduct and our pull request review process.
The TL;DR is send us a pull request, iterate on the feedback + discussion, and get a +1 from a [Committer](COMMITTERS.md) in order to get your PR accepted.

The current list of active committers (who can +1 a pull request) can be found here: [Committers](COMMITTERS.md)

A list of contributors to the project can be found here: [Contributors](https://github.com/twitter/storehaus/graphs/contributors)

## Maven

Storehaus modules are available on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.13.0`.

Current published artifacts are

* `storehaus-core_2.11`
* `storehaus-core_2.10`
* `storehaus-algebra_2.11`
* `storehaus-algebra_2.10`
* `storehaus-memcache_2.11`
* `storehaus-memcache_2.10`
* `storehaus-mysql_2.11`
* `storehaus-mysql_2.10`
* `storehaus-hbase_2.11`
* `storehaus-hbase_2.10`
* `storehaus-redis_2.11`
* `storehaus-redis_2.10`
* `storehaus-dynamodb_2.11`
* `storehaus-dynamodb_2.10`
* `storehaus-kafka-08_2.11`
* `storehaus-kafka-08_2.10`
* `storehaus-mongodb_2.11`
* `storehaus-mongodb_2.10`
* `storehaus-elasticsearch_2.11`
* `storehaus-elasticsearch_2.10`
* `storehaus-leveldb_2.11`
* `storehaus-leveldb_2.10`
* `storehaus-http_2.11`
* `storehaus-http_2.10`
* `storehaus-cache_2.11`
* `storehaus-cache_2.10`
* `storehaus-testing_2.11`
* `storehaus-testing_2.10`

The suffix denotes the scala version.

## Testing notes

We use `travis-ci` to set up any underlying stores (e.g. MySQL, Redis, Memcached) for the tests. In order for these tests to pass on your local machine, you may need additional setup.

### MySQL tests

You will need MySQL installed on your local machine.
Once installed, run the `mysql` commands listed in [.travis.yml](https://github.com/twitter/storehaus/blob/develop/.travis.yml) file.

### Redis tests

You will need [redis](http://redis.io/) installed on your local machine. Redis comes bundled with an executable for spinning up a server called `redis-server`. The Storehaus redis tests expect the factory defaults for connecting to one of these redis server instances, resolvable on `localhost` port `6379`.

### Memcached

You will need [Memcached](http://memcached.org/) installed on your local machine and running on the default port `11211`.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>

## Contributors

Here are a few that shine among the many:

* Ruban Monu <https://twitter.com/rubanm>, for `storehaus-mysql`
* Doug Tangren <https://twitter.com/softprops>, for `storehaus-redis`
* Ryan Weald <https://twitter.com/rweald>, for `storehaus-dynamodb`

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
