Storehaus-Cascading
===================
According to the Cascading-website "Cascading is the proven application development platform for building data applications on Hadoop." [Cascading-Website](http://www.cascading.org). In Cascading so called "Taps" are used to define data sources and sinks. Using class StorehausTap Storehaus can be used in conjunction with Cascading (MapReduce/Hadoop).

Sinks
-----
Any Storehaus-WritableStore can be used as a Cascading-sink (SinkTap). Cascading will be able to sink data conforming to the stores contract into the store. To achieve that static store intializers can be passed to the constructor of StorehausTap to circumvent serialization of store objects in Hadoop. In terms of Scala this means that instantiation is wrapped in an "object" which implements StorehausCascadingInitializer. Every job using StorehausTap will provide such objects in its jar which Hadoop distributes across the cluster. Jobs can provide several of those within the same job.

Storing allows two modes of operation, which can be set globally for all StorehausTaps:
  * Synchronous: The system will wait for every write to complete, which is the safest mode of operation (default). 
  * Asynchronous: Writes will be executed asynchronously, striving for higher parallelism (and may be faster, but may lose a few updates if the store is not correctly shut down). Set com.twitter.storehaus.cascading.outputformat.forcefuture in JobConf to "true" to use this.

Sources
-------
Cascading sources provide cascading-tuples to pipes. Since most ReadableStores do not provide key- or tuple-streams keys must be provided by other means other than ReadableStore.get. This is achieved using SplittingMechanism, which requires that splits can be calculated and data can be enumerated. The following SplittingMechanism are being provided:
  * JobConf-Key-Based: keys must be provided as part of JobConf. Every ReadableStore can be used with this SplittingMechanism (default), but it is limited in space and splits are enumerate all keys on the client. See object JobConfKeyArraySplittingMechanism for how to provide the keys and serialization.
  * SplittableStore-Based: SplittableStores can be split up into sub-ranges or sub-stores, called splits. There are default implementations for IterableStores and QueryableStores.
  * Specific SplittingMechanisms: Some stores provide specific SplittingMechanisms which are more specifically designed for the use case at hand. The only example available at the time writing is the family of CassandraStores which provide CassandraSplittingMechanism.

To set a splitting mechanism use StorehausInputFormat.setSplittingClass on JobConf.

Scalding and Summingbird
------------------------
There are implementations for Scalding sources and Summingbrid VersionedBatchedStores (at the time of writing not yet PRed, though). 

How to use?
-----------
Project storehaus-cascading-examples contains a few examples which makes usage more clear. 
