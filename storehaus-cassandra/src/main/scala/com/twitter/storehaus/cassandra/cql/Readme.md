Storehaus-Cassandra
===================

Store support matrix
--------------------

Store | ReadableStore | WritableStore | MergableStore | IterableStore | QueryableStore | CASStore | WithPutTtl
--------------------------------------------------------------------------------------------------------------
CQLCassandraStore | x | x | | x | x | x | x
CQLCassandraCompositeStore | x | x | | x | x | x | x
CQLCassandraCollectionStore | x | x | x | x | x | x | x
CQLCassandraLongStore | x | (x) | x | x | x | | 
CassandraTupleStore | x | (x) | | x | x | (x) |

Notes
-----
  * All stores require storable items (key-parts and values) to conform to [CassandraPrimitive](https://github.com/websudos/phantom/blob/develop/phantom-dsl/src/main/scala/com/websudos/phantom/CassandraType.scala)
  * All stores using [HLists]() as keys can be passed to [CassandraTupleStore]() to make them more easily accessible to serialization and classical Scalding and Summingbird jobs which often use tuples. However, restrictions of the underlying stores (e.g. LongStore) still apply.
  * All stores can be used with [Cascading]() using StorehausTap as Sources and Sinks. Use [CassandraSplittingMechanism]() to use them as input to Hadoop mappers (see storehaus-cascading).
  * Merging of Lists and Sets (see [CQLCassandraCollectionStore]()) means that elements are concatenated (Lists) or conjugated (Sets)
  * Merging of Longs (see [CQLCassandraLongStore]()) means that Longs use "+" to be merged on store basis, but there are some troubles using put operations (must be synchronized externally) and retrieving old values before the merged (same problem)

