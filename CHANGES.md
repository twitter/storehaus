# storehaus #

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
