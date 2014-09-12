Storehaus-Cassandra
===================

Storehaus-Stores wrapping [Datastax' JAVA Driver](https://github.com/datastax/java-driver) and [Phantoms](https://github.com/websudos/phantom/) serialization of primitive types.

Besides a simple key-value store [CQLCassandraStore](CQLCassandraStore.scala) (and also a slightly more complex [CQLCassandraMultivalueStore](CQLCassandraMultivalueStore.scala) which takes Tuples as values) there are stores supporting composite partition- and clustering-keys [CQLCassandraCompositeStore](CQLCassandraCompositeStore.scala), [CQLCassandraCollectionStore](CQLCassandraCollectionStore.scala), [CQLCassandraMultivalueStore](CQLCassandraMultivalueStore.scala) and [CQLCassandraLongStore](CQLCassandraLongStore.scala). These keys are provided as a Tuple2 of a [HList](https://github.com/milessabin/shapeless) of partition-keys and a HList of clustering keys.

Supported Store Traits
----------------------

Store | ReadableStore | WritableStore | MergableStore | IterableStore | QueryableStore | CASStore | WithPutTtl
------|---------------|---------------|---------------|---------------|----------------|----------|-----------
CQLCassandraStore | x | x | | x | x | x | x
CQLCassandraMultivalueStore | x | x | | x | x | x | x
CQLCassandraCompositeStore | x | x | | x | x | x | x
CQLCassandraCollectionStore | x | x | x | x | x | x | x
CQLCassandraLongStore | x | (x) | x | x | x | | 
CQLCassandraMultivalueStore | x | x | | x | x | x | x
CassandraTuple*Store | x | (x) | | x | x | |

Some Notes
----------
  * All stores require storable items (key-parts and values) to conform to [CassandraPrimitive](https://github.com/websudos/phantom/blob/develop/phantom-dsl/src/main/scala/com/websudos/phantom/CassandraType.scala). com.websudos.phantom.CassandraPrimitive should be imported to provide implicits for basic Scala types.
  * Some stores require a lot of implicits. Most of them are provided using [Shapeless](https://github.com/milessabin/shapeless). shapeless._ should be imported in client code.
  * All stores using [HLists](https://github.com/milessabin/shapeless) as keys can be passed to [CassandraTupleStore](cassandraTupleStores.scala) to make them more easily accessible to serialization and classical Scalding and Summingbird jobs which often use tuples. However, restrictions of the underlying stores (e.g. LongStore) still apply. Its counterpart [CassandraTupleMultiValueStore](cassandraTupleStores.scala) is used for Tuple-based values.
  * All stores can be used with [Cascading](http://www.cascading.org/) using StorehausTap as Sources and Sinks. Use [CassandraSplittingMechanism](cascading/CassandraSplittingMechanism.scala) to use them as input to Hadoop mappers (see storehaus-cascading).
  * Merging of Lists and Sets (see [CQLCassandraCollectionStore](CQLCassandraCollectionStore.scala)) means that elements are concatenated (Lists) or conjugated (Sets)
  * Merging of Longs (see [CQLCassandraLongStore](CQLCassandraLongStore.scala)) means that Longs use "+" to be merged on store basis, but there are some troubles using put operations (must be synchronized externally) and retrieving old values before the merged (same problem)
  * This implementation of [QueryableStore](https://github.com/twitter/storehaus/blob/develop/sotrehaus-core/src/main/scala/com/twitter/storehaus/QueryableStore.scala) does not return only values as defined by its interface, but also returns the keys as "(K, V)". The interface allows to provide additional where clauses as a single String.
