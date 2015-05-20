# Storehaus #

### Version 0.11.0 ###
* Add correct String/ChannelBuffer injections #257
* initial scalatest migration #260
* Remove usage of twitter's maven repo, travis seems to dislike it -- mayb... #261
* Bijection 0.8.0, algebird 0.10.0, scalding 0.14.0, and scala 2.10.5

### Version 0.10.0 ###
* Use latest scalding, algebird, and bijection versions: https://github.com/twitter/storehaus/pull/255
* Use new Travis CI container infrastructure: https://github.com/twitter/storehaus/pull/254
* Add hook for CAS based memcache mergeable: https://github.com/twitter/storehaus/pull/252
* Bump bijection/algebird versions: https://github.com/twitter/storehaus/pull/253
* Remove + operator: https://github.com/twitter/storehaus/pull/21
* Memcache mergeable - use semigroup: https://github.com/twitter/storehaus/pull/251
* add logic for replicating writes and reads to stores: https://github.com/twitter/storehaus/pull/20
* bump finagle and util to 6.22.0: https://github.com/twitter/storehaus/pull/247
* Minified kill 2.9.3: https://github.com/twitter/storehaus/pull/249
* Read through store - do not query backing store when no cache miss: https://github.com/twitter/storehaus/pull/246
* implementation of store that uses http protocol: https://github.com/twitter/storehaus/pull/241
* Retry unittest: https://github.com/twitter/storehaus/pull/240
* Added endpoint support to storehaus-dynamodb: https://github.com/twitter/storehaus/pull/236
* Https sonatype: https://github.com/twitter/storehaus/pull/237

### Version 0.9.1 ###
* Feature/write through cache perf: https://github.com/twitter/storehaus/pull/234
* Share the Retrying Read Write store in storehaus repo: https://github.com/twitter/storehaus/pull/230
* initial Kafka 0.8 support: https://github.com/twitter/storehaus/pull/232
* Exceptions on the cache-store should be ignored for Read/WriteThroughStore: https://github.com/twitter/storehaus/pull/225

### Version 0.9.0 ###
* Reporting store algebra: https://github.com/twitter/storehaus/pull/176
* Bumping finagle to a more recent version, changes that were required: https://github.com/twitter/storehaus/pull/223
* Bump Algebird to version 0.5.0: https://github.com/twitter/storehaus/pull/221
* Add stores for read-through and write-through caching: https://github.com/twitter/storehaus/pull/220
* fix bug in onFailure enriched mergeable store: https://github.com/twitter/storehaus/pull/218
* Fixes an issue that Future.collect is N^2 on scala Lists: https://github.com/twitter/storehaus/pull/219
* Adds GetBatchingReadableStore: https://github.com/twitter/storehaus/pull/215
* Elastic Search Store: https://github.com/twitter/storehaus/pull/205
* Issue #72: Added mongodb store.: https://github.com/twitter/storehaus/pull/199
* Add out of retries exception to retrying store: https://github.com/twitter/storehaus/pull/210
* IterableStore: https://github.com/twitter/storehaus/pull/191
* add onFailure to EnrichedMergeableStore: https://github.com/twitter/storehaus/pull/200
* clean up htable after finishing get and put operations.: https://github.com/twitter/storehaus/pull/207
* Adds a mutable TTL cache: https://github.com/twitter/storehaus/pull/196
* add MergeableStore.fromStoreNoMulti that does single get then put: https://github.com/twitter/storehaus/pull/201
* my little proxy: https://github.com/twitter/storehaus/pull/202
* Add immutable LIRS Cache implementation: https://github.com/twitter/storehaus/pull/155
* Adds the CalendarTimeStrategy: https://github.com/twitter/storehaus/pull/195
* Adds the ability to add an Optional component onto any strategy: https://github.com/twitter/storehaus/pull/198
* Just adds some whitespace: https://github.com/twitter/storehaus/pull/197
* Kafka Sink for SummingBird: https://github.com/twitter/storehaus/pull/192

### Version 0.8.0 ###
* add BatchedStore for writes: https://github.com/twitter/storehaus/pull/175
* MySQL batched multiPut: https://github.com/twitter/storehaus/pull/173
* MergeableMemcacheStore: https://github.com/twitter/storehaus/pull/182
* Writeable stores: https://github.com/twitter/storehaus/pull/180
* Add readme notes for memcached tests: https://github.com/twitter/storehaus/pull/184
* Add the Mergeable type: https://github.com/twitter/storehaus/pull/183
* MergeableMySqlStore and MySqlLongStore: https://github.com/twitter/storehaus/pull/181
* PivotedStore: https://github.com/twitter/storehaus/pull/186
* Make twitter util provided: https://github.com/twitter/storehaus/pull/190
* Remove version file: https://github.com/twitter/storehaus/pull/194

### Version 0.7.1 ###
* Remove sources req on specs2, breaks downstream deps

### Version 0.7.1 ###
* Upgrade to specs2, include sbt runner: https://github.com/twitter/storehaus/pull/170

### Version 0.7.0 ###
* Read from two stores concurrently: https://github.com/twitter/storehaus/pull/158
* Merge returns the value before: https://github.com/twitter/storehaus/pull/163
* Mergeable uses Semigroup (swap store is possible): https://github.com/twitter/storehaus/pull/165
* Better sorted set support in redis: https://github.com/twitter/storehaus/pull/152
* Move from java Closeable to twitter.util.Closable (Future[Unit] return)

### Version.0.6.0 ###
* Fix readme link to mysql store: https://github.com/twitter/storehaus/pull/143
* Tunable Replicated Store: https://github.com/twitter/storehaus/pull/142
* Add Community Section to README: https://github.com/twitter/storehaus/pull/138
* DynamoDB Store: https://github.com/twitter/storehaus/pull/144
* Move travis to build with 2.10.2 : https://github.com/twitter/storehaus/pull/150
* Added AsyncHBase based Store: https://github.com/twitter/storehaus/pull/149
* Redis sorted sets: https://github.com/twitter/storehaus/pull/127
* Don't catch all Throwable: https://github.com/twitter/storehaus/pull/148

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
