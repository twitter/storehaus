## Storehaus [![Build Status](https://secure.travis-ci.org/twitter/storehaus.png)](http://travis-ci.org/twitter/storehaus)

Storehaus is a library that makes it easy to work with remote key value stores.

### Storehaus-Core

Storehaus's core module defines two traits; a read-only `ReadableStore` and a read-write `Store`. The traits themselves are tiny:

```scala
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

The `ReadableStore` object gives you a bunch of ways to create new ReadableStore instances. fromMap, retc.

Discuss Combinators -- ReadableStore.{ andThen, convert, first }

Core gives you a bunch of ways to create these stores. Algebra gives you the combinators.

### Storehaus-Algebra

Talk about the combinators. ReadableStoreAlgebra

```scala
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.ReadableStoreAlgebra._

val store = ReadableStore.fromMap(Map[Int, String](1 -> "some value", 2 -> "other value"))

store.get(1).get
// res5: Option[String] = Some(some value)

// The following store maps an integer to the count of the string value:
val countStore: ReadableStore[Int, Int] = store.mapValues { s => s.size }

countStore.get(1).get
// res6: Option[Int] = Some(10)

// Store ops

import com.twitter.storehaus.{ JMapStore, Store }
import com.twitter.storehaus.algebra.StoreAlgebra._

val pivotedStore = new JMapStore[Int, Map[Int, String]]
val unpivoted: Store[(Int, Int), String] = pivotedStore.unpivot(identity)

unpivoted.multiPut(Map((1, 0) -> Some("one"), (1, 5) -> Some("two"), (1, 2) -> Some("three"), (2, 0) -> Some("four")))

unpivoted.get((1, 1)).get
// res3: Option[String] = Some(two)

scala> pivotedStore.get(1).get
// res5: Option[Map[Int,String]] = Some(Map(5 -> two, 0 -> one, 1 -> two, 2 -> three))

// Add a new value:
unpivoted.put((1, 0) -> Some("new Value"))

unpivoted.get((1, 0)).get
// res8: Option[String] = Some(new Value)

pivotedStore.get(1).get
// res9: Option[Map[Int,String]] = Some(Map(5 -> two, 0 -> new Value, 1 -> two, 2 -> three))
```

#### MergeableStore

```scala
trait Mergeable[-K, V] extends java.io.Serializable {
  def monoid: Monoid[V]
  def merge(kv: (K, V)): Future[Unit]
  def multiMerge[K1 <: K](kvs: Map[K1,V]): Map[K1, Future[Unit]]
}

trait MergeableStore[-K, V] extends Mergeable[K, V] with Store[K, V]
```

### Other Modules

Storehaus-memcache wraps Twitter's [finagle-memcached](https://github.com/twitter/finagle/tree/master/finagle-memcached) library. We plan on writing modules for (link to the issues for redis, etc)

### Documentation

See the [current API documentation](http://twitter.github.com/storehaus) for more information.

## Maven

Current version is `0.1.0`. groupid=`"com.twitter"` artifact=`storehaus_2.9.2"`.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
