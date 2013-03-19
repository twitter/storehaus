## Storehaus [![Build Status](https://secure.travis-ci.org/twitter/storehaus.png)](http://travis-ci.org/twitter/storehaus)

Storehaus is a library that makes it easy to work with asynchronous key value stores. Storehaus is built on top of Twitter's [Future](https://github.com/twitter/util/blob/master/util-core/src/main/scala/com/twitter/util/Future.scala).

### Storehaus-Core

Storehaus's core module defines four traits:

```scala
package com.twitter.storehaus

// Read-only store
trait ReadableStore[-K, +V] extends Closeable {
  def get(k: K): Future[Option[V]]
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]]
  override def close { }
}

// read-write async store.
trait Store[-K, V] extends ReadableStore[K, V] {
  def put(kv: (K, Option[V])): Future[Unit]
  def multiPut[K1 <: K](kvs: Map[K1, Option[V]]): Map[K1, Future[Unit]]
}

// write-only async store/
trait Mergeable[-K, V] extends Serializable {
  def merge(kv: (K, V)): Future[Unit]
  def multiMerge[K1<:K](kvs: Map[K1,V]): Map[K1, Future[Unit]]
}

// read-write async store. (If you're using key-value stores for
// aggregations, you're going to love MergeableStore.)
trait MergeableStore[-K, V] extends Mergeable[K,V] with Store[K, V]
```

The `ReadableStore` trait uses the `Future[Option[V]]` return type to communicate one of three states about each value. A value is either

* definitely present,
* definitely missing, or
* unknown due to some error (perhaps a timeout, or a downed host).

The [`ReadableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.ReadableStore$) and [`Store`](http://twitter.github.com/storehaus/#com.twitter.storehaus.Store$) companion objects provide a bunch of ways to create new stores. See the linked API documentation for more information.

`Mergeable`'s `merge` and `multiMerge` are similar to `put` and `multiPut`; the difference is that values added with `merge` are added to the store's existing value. The `storehaus-algebra` module (described below) provides tools to create `MergeableStore` instances from existing `Store`s using a `Monoid[V]` from Twitter's [Algebird](https://github.com/twitter/algebird) project.

The [`Mergeable`](http://twitter.github.com/storehaus/#com.twitter.storehaus.algebra.Mergeable$) and [`MergeableStore`](http://twitter.github.com/storehaus/#com.twitter.storehaus.algebra.MergeableStore$) objects provide a number of combinators on their respective types. For ease of use, Storehaus provides implicit conversions to enrichments on `Mergeable` and `MergeableStore`. Access these by importing, respectively, `Mergeable.enrich` or `MergeableStore.enrich`.

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

`storehaus-core` helps you create stores; `storehaus-algebra` gives you the tools to combine stores in interesting ways.

#### MergeableStore.fromStore

`MergeableStoreAlgebra.fromStore` allows you to create a `MergeableStore` from an existing `Store` by taking advantage of that store's `get` and `put` methods. Because the addition of the merged delta is handled with a `Monoid[V]` from Twitter's [Algebird](https://github.com/twitter/algebird) project, it's easy to write stores that aggregate [Lists](http://twitter.github.com/algebird/#com.twitter.algebird.ListMonoid), [decayed values](http://twitter.github.com/algebird/#com.twitter.algebird.DecayedValue), even [HyperLogLog](http://twitter.github.com/algebird/#com.twitter.algebird.HyperLogLog$) instances.

### Other Modules

Storehaus provides a number of modules wrapping existing key-value stores. Enriching these key-value stores with Storehaus's combinators has been hugely helpful to us here at Twitter. Writing your jobs in terms of Storehaus stores makes it easy to test your jobs; use an in-memory `JMapStore` in testing and a `MemcacheStore` in production.

* [Storehaus-memcache](http://twitter.github.com/storehaus/#com.twitter.storehaus.memcache.MemcacheStore) (wraps Twitter's [finagle-memcached](https://github.com/twitter/finagle/tree/master/finagle-memcached) library)

#### Planned Modules

Here's a list of modules we plan in implementing, with links to the github issues tracking progress on these modules:

* [storehaus-leveldb](https://github.com/twitter/storehaus/issues/51)
* [storehaus-berkeleydb](https://github.com/twitter/storehaus/issues/52)
* [storehaus-redis](https://github.com/twitter/storehaus/issues/53)
* [storehaus-mysql](https://github.com/twitter/storehaus/issues/59)
* [storehaus-dynamodb](https://github.com/twitter/storehaus/issues/60)

### Documentation

See the [current API documentation](http://twitter.github.com/storehaus) for more information.

## Maven

Storehaus modules are available on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.1.0`.

Current published artifacts are

* `storehaus-core_2.9.2`
* `storehaus-core_2.10`
* `storehaus-algebra_2.9.2`
* `storehaus-algebra_2.10`
* `storehaus-memcache_2.9.2`
* `storehaus-memcache_2.10`

The suffix denotes the scala version.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
