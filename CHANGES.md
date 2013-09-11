# storehaus #

### Version.0.5.1 ###

* Add storehaus-hbase and upgrade to bijection 0.5.3: https://github.com/twitter/storehaus/pull/139
* Fix mutable TTL cache bug: https://github.com/twitter/storehaus/pull/136

### Version.0.5.0 ###

* Reuse prepared statements in mysql: https://github.com/twitter/storehaus/issues/93
* storehaus-testing module: https://github.com/twitter/storehaus/pull/115
* cache ttl is now a duration, vs a time: https://github.com/twitter/storehaus/pull/100
* improve performance of CollectionOps: https://github.com/twitter/storehaus/pull/117
* Augment memcachestore with common functions: https://github.com/twitter/storehaus/pull/121
* bump twitter-util and finagle versions: https://github.com/twitter/storehaus/pull/125
* Upgrade to scala 2.9.3, algebird 0.2.0 and Bijection 0.5.2: https://github.com/twitter/storehaus/pull/126

Thanks to Doug Tangren, Ruban Monu, Ximing Yu, Ryan LeCompte, Sam Ritchie and Oscar Boykin for contributions!

### Version.0.4.0 ###

* Storehaus-Mysql support for numeric types
* Name change in mysql store

### Version.0.3.0 ###

* Adds storehaus-mysql
* Adds storehaus-cache
* Adds storehaus-redis
* RetryingReadableStore
* CachedReadableStore
* CachedReadableStore
* ReadableStore.find (and SearchingReadableStore)

### Version.0.2.0 ###

* Move `MergeableStore` into storehaus-core
* Remove `Mergeable`
* Prep for open source release.

### Version.0.1.0 ###

* Documentation overhaul
* `BufferingStore`
* `UnpivotedStore`
* `FutureCollector`

### Version 0.0.4 ###

* Break `Store` into `Store` and `MergeableStore`.
* Massive API cleanup.

### Version 0.0.3 ###

* Change up `Store` interface's multiGet.
* ChannelBuffer bijection.

### Version 0.0.2 ###

* `storehaus-algebra`
* `multiGet` returns `Map[K, Option[V]]`

### Version 0.0.1 ###

* Basic store traits.
* `storehaus-memcache`
