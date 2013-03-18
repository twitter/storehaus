## Storehaus [![Build Status](https://secure.travis-ci.org/twitter/storehaus.png)](http://travis-ci.org/twitter/storehaus)

Storehaus is a library that makes it easy to work with remote key value stores. Storehaus's core module defines two traits; a read-only `ReadableStore` and a read-write `Store`. The traits themselves are dirt simple:

```scala
trait ReadableStore[-K, +V] extends Closeable {
  def get(k: K): Future[Option[V]]
  def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]]
  override def close { }
}


See the [current API documentation](http://twitter.github.com/storehaus) for more information.

## Maven

Current version is `0.1.0`. groupid=`"com.twitter"` artifact=`storehaus_2.9.2"`.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
