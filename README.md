## Storehaus [![Build Status](https://secure.travis-ci.org/twitter/storehaus.png)](http://travis-ci.org/twitter/storehaus)

Storehaus is a library that makes it easy to work with asynchronous key value stores. Storehaus is built on top of Twitter's [Future](https://github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/util/Future.scala).

### Storehaus-Core

Storehaus's core module defines two traits; a read-only `ReadableStore` and a read-write `Store`. The traits themselves are tiny:

```scala
package com.twitter.storehaus

trait ReadableStore[-K, +V] extends Closeable {
  def get(k: K): Future[Option[V]]
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]]
  override def close { }
}

trait Store[-K, V] extends ReadableStore[K, V] {
  def put(kv: (K, Option[V])): Future[Unit]
  def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]]
}
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
  def merge(kv: (K, V)): Future[Unit] = multiMerge(Map(kv)).apply(kv._1)
  def multiMerge[K1 <: K](kvs: Map[K1, V]): Map[K1, Future[Unit]] = kvs.map { kv => (kv._1, merge(kv)) }
}
```

`MergeableStore`'s `merge` and `multiMerge` are similar to `put` and `multiPut`; the difference is that values added with `merge` are added to the store's existing value. Because the addition is handled with a `Monoid[V]` from Twitter's [Algebird](https://github.com/twitter/algebird) project, it's easy to write stores that aggregate [Lists](http://twitter.github.com/algebird/#com.twitter.algebird.ListMonoid), [decayed values](http://twitter.github.com/algebird/#com.twitter.algebird.DecayedValue), even [HyperLogLog](http://twitter.github.com/algebird/#com.twitter.algebird.HyperLogLog$) instances.

The [`MergeableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.algebra.MergeableStore$) object provides a number of combinators on these stores. For ease of use, Storehaus provides an implicit conversion to an enrichment on `MergeableStore`. Access this by importing `MergeableStore.enrich`.

### Other Modules

Storehaus provides a number of modules wrapping existing key-value stores. Enriching these key-value stores with Storehaus's combinators has been hugely helpful to us here at Twitter. Writing your jobs in terms of Storehaus stores makes it easy to test your jobs; use an in-memory `JMapStore` in testing and a `MemcacheStore` in production.

  * [Storehaus-memcache](http://twitter.github.com/storehaus/#com.twitter.storehaus.memcache.MemcacheStore) (wraps Twitter's [finagle-memcached](https://github.com/twitter/finagle/tree/master/finagle-memcached) library)
  * [Storehaus-mysql](http://twitter.github.com/storehaus/#com.twitter.storehaus.mysql.MySQLStore) (wraps Twitter's [finagle-mysql](https://github.com/twitter/finagle/tree/master/finagle-mysql) library)
  * [Storehaus-redis](http://twitter.github.com/storehaus/#com.twitter.storehaus.redis.RedisStore) (wraps Twitter's [finagle-redis](https://github.com/twitter/finagle/tree/master/finagle-redis) library)

#### Planned Modules

Here's a list of modules we plan in implementing, with links to the github issues tracking progress on these modules:

* [storehaus-leveldb](https://github.com/twitter/storehaus/issues/51)
* [storehaus-berkeleydb](https://github.com/twitter/storehaus/issues/52)
* [storehaus-dynamodb](https://github.com/twitter/storehaus/issues/60)

## Community and Documentation

To learn more and find links to tutorials and information around the web, check out the [Storehaus Wiki](https://github.com/twitter/storehaus/wiki).

The latest ScalaDocs are hosted on Storehaus's [Github Project Page](http://twitter.github.io/storehaus).

Discussion occurs primarily on the [Storehaus mailing list](http://librelist.com/browser/storehaus/). To join the mailing list, email [storehaus@librelist.com](mailto:storehaus@librelist.com). The same address is used for posting once you've joined. Issues should be reported on the [GitHub issue tracker](https://github.com/twitter/storehaus/issues).

## Maven

Storehaus modules are available on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.5.0`.

Current published artifacts are

* `storehaus-core_2.9.3`
* `storehaus-core_2.10`
* `storehaus-algebra_2.9.3`
* `storehaus-algebra_2.10`
* `storehaus-memcache_2.9.3`
* `storehaus-memcache_2.10`
* `storehaus-mysql_2.9.3`
* `storehaus-mysql_2.10`
* `storehaus-redis_2.9.3`
* `storehaus-redis_2.10`
* `storehaus-cache_2.9.3`
* `storehaus-cache_2.10`
* `storehaus-testing_2.9.3`
* `storehaus-testing_2.10`

The suffix denotes the scala version.

## Testing notes

We use travis-ci to set up any underlying stores (e.g. MySQL and Redis) for the tests. In order for these tests to pass on your local machine, you may need additional setup.

### MySQL tests

You will need MySQL installed on your local machine.
Once installed, run the `mysql` commands listed in [.travis.yml](https://github.com/twitter/storehaus/blob/develop/.travis.yml) file.

### Redis tests

You will need [redis](http://redis.io/) installed on your local machine. Redis comes bundled with an executable for spinning up a server called `redis-server`. The Storehaus redis tests expect the factory defaults for connecting to one of these redis server instances, resolvable on `localhost` port `6379`.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>

## Contributors

Here are a few that shine among the many:

* Ruban Monu <https://twitter.com/rubanm>, for `storehaus-mysql`
* Doug Tangren <https://twitter.com/softprops>, for `storehaus-redis`

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
