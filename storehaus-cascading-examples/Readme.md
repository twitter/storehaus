Tiny Storehaus-Cascading getting started
========================================

This is a tiny getting started with the combination of Storehaus-stores and Cascading. It comprises very basic examples to start from.

Prerequesites
-------------
  * Prepare a Hadoop-cluster. Version 1.2.1 is known to work. Run at least one of the examples coming with the Hadoop-distribution to test if it is really working.
  * It is advisable to make sure users you are going to use have the rights to execute mapreduce and mangle data on HDFS.
  * You should have basic Cascading and programming knowledge (if not you can just go through a Cascading tutorial).
  * Install Cassandra somewhere and change the settings (i.e. IP-address, thrift-port and name of the keyspace) in ```ExampleConfigurationSettings.scala``` accordingly.
  * For the later examples please install memcached (or refer to an installation which is already available at your site).
  * Execute a ```sbt -210 clean publish-local``` and then a ```sbt -210 "project storehaus-cascading-examples" clean compile assembly``` in storehaus's main git directory. This builds an uber-jar based on Scala version 2.10.3 which can be submitted to hadoop.

Example 1: A WritableStore for Cascading
----------------------------------------
  
Upload some input file to HDFS. The input file should reside in /tmp/test.in. E.g. in storehaus-cascading-examples directory do a ```hadoop fs -put src/main/resources/test.in /tmp/test.in``` to upload the text-file provided with these examples.

To execute the Cascading job please run ```hadoop jar target/scala-2.10/storehaus-cascading-examples-0.9.1.jar com.twitter.storehaus.cascading.examples.StorehausCascadingExample1WritableStore```. 

  * Example 1 is a modified copy-over job from the original Cascading tutorial, step 1. We will also use it for some of the other examples as well.

When the job is finished you should be able to run ```cqlsh <your-cassandra-ip> -k mytestkeyspace -f src/main/resources/example1.cql``` (adapting your-cassandra-ip and mytestkeyspace). This will display the results, which is a table with the contents of test.in.

  * Have a look at the job. You'll notice 3 things:
    * An object called ```SimpleStoreInitializer``` implementing ```StorehausCascadingInitializer[String, String]```. It has a method ```getWritableStore``` which returns a WritableStore from Storehaus. These stores have a ```put``` method to store some data item in them.
    * In the job itself there is some ```inTap``` which actually is a ```Hfs[TextDelimited]``` reading plain-text from the file we just uploaded. Thereby it provides all the data for our job.
    * Also, there is some ```outTap`` which is initialized with ```StorehausTap[String, String](SimpleStoreInitializer)```. So this is related to the object with the store. It is a Cascading-Tap sinking all the data from the flow.

To summarize: SimpleStoreInitializer (and any other StorehausCascadingInitializer) is used to initialize WritableStore on client or slave machines. It must be an object, such that the Store can be initialized on Hadoop slaves without actually serializing the store. This allows to use more stores: In ```SimpleStoreInitializer``` you can replace the ```store``` with any WritableStore from Storehaus and the example will still work, now sinking to your store of choice (please change prepareStore, too, to make sure all resources required by the store get initialized).

Example 2: Creating a source with Storehaus-Cascading
-----------------------------------------------------

The second example is just like the first with one difference: It also reads data from your Cassandra installation.
Use ```hadoop jar target/scala-2.10/storehaus-cascading-examples-0.9.1.jar com.twitter.storehaus.cascading.examples.StorehausCascadingExample2Store``` to run the job.

   * To use the native Cassandra-Hadoop-Spliiting-Mechanism which is better if using Cassandra than the default storehaus-cascading ones ```AppProps.setApplicationJarClass( properties, StorehausCascadingExample2Store.getClass )``` is writing a new splitting-mechanism into JobConf.
   * To support this type of SplittingMechanism the object now also implements ```CassandraCascadingInitializer``` adding a bunch of methods which already have been implemented in the object.
   * Except for the definition of ```inTap```, there are not many changes to the job itself. InTap now also points to the Initializer.

To summarize: This job is pretty useless as it reads data and overwrites it with the same data, but it shows that it's possible to read and write data from the same store. For that job to work, we need 2 store intializers (Example 3) or modify the keys in the job to write into different rows (left as an excercise for the experienced Cascading user).

Example 3: Using different input and output-stores, Using Scala-tuples.
-----------------------------------------------------------------------

The third example splits up the second one. Thereby it copies data into a new comlumn family instead of overwriting the data.

   * Scala-tuples are floating in the job. In this case this is achieved by using the stores together with a store-specific wrapper for tuples. Other more generic approaches could also use bijection/injections or a custom serialization mechanism (summingbird has an example of how to use tuples with stores and bijection).
   * Internally Storehaus-Cascading puts new keys into JobConf for each Tap. At a first glance they would be overwritten, when adding new stores/initializers. But Cascading creates a new JobConf for each Tap, so the keys of Storehaus-Cascading don't interfere with each other.

To summarize: Using multiple store initializers is straight forward. Using Scala-tuples can be done easily, if the store supports or has appropriate wrappers for them (which probably is what you want if you continue to use storehaus-cascading with Scalding). Note however, that Hadoop/Cascading need to be able to serialize your data (at least if using Reducers), so if writing jobs with reducers serialization must be set up.

Example 4: Using Reducers, an IterableStore as a Source and a Memcached-Sink
----------------------------------------------------------------------------

It is time to install a memcached now, which should be made accessible by hadoop slave nodes. ```ExampleConfigurationSettings.scala``` must be changed to reflect the IP of the memcached server. The assembly-jar for the jobs needs to a re-build: ```sbt -210 "project storehaus-cascading-examples" clean compile assembly```. Finally, the job runs with the command ```hadoop jar storehaus-cascading-examples/target/scala-2.10/storehaus-cascading-examples-0.9.1.jar com.twitter.storehaus.cascading.examples.StorehausCascadingIterableExample```.

   * SplittableStore is a combinator for IterableStores that allows splitting up the store into peaces, which is needed by Hadoop to perform mappings in parallel (which is probably why you use Hadoop in the first place).
   * A different SplittingMechanism is in place: ```StorehausInputFormat.setSplittingClass[String, String, StoreInitializerMapStoreClass, MapStoreClassMechanism](conf, classOf[MapStoreClassMechanism])```
   * To ease life a bit, the types for the splitting mechanism and the corresponding stores are defined in ```package.scala```.

The downside of the default SplittingMechanism based on IterableSplittableStore is that it needs to seek to the start-position of the data represented by this split. Iterators do not have cursors, so there is no chance of doing a direct seek with an Iterator.

Example 5: Using a very simple JobConf-Splitting Mechanism and Memcached as a source
------------------------------------------------------------------------------------

This example reads from Memcached using a pre-defined set of keys which has been written into JobConf. You shouldn't store too many data (and therefore too many keys) in JobConf. So this is best used if number(keys)/size(values) is almost zero, e.g. values are previously stored arrays.

   * KeyBasedJobConfSplittingMechanism can be used to split any Storehaus-store.
   * KeyBasedJobconfSplittingMechanism can be used if the job only gets a small number of keys from the store.
   * The mechanism can easily be extended to read a set of larger files with keys from HDFS.
