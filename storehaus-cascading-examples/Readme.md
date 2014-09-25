Tiny Storehaus-Cascading getting started
========================================

This is a 3 step tutorial how to get started with Storehaus and Cascading. It comprises 3 very basic examples.

Prerequesites
-------------
  * The first step is always to get a running Hadoop-cluster. Version 1.2.1 is known to work. Run at least the WordCount examples coming with the distribution to test if it is really working.
  * Make sure your users have the rights to execute mapreduce and  mangle data on HDFS
  * You should have basic Cascading and programming knowledge (if not you can just go through a Cascading tutorial).
  * Install Cassandra somewhere and change the settings (i.e. IP-address, thrift-port and name of the keyspace) in ```ExampleConfigurationSettings.scala``` accordingly.
  * Execute a ```sbt -210 clean publish-local``` and then a ```sbt -210 "project storehaus-cascading-examples" compile assembly``` in storehaus's main git directory. This builds an uber-jar based on Scala version 2.10.3 which can be submitted to hadoop.

Example 1: A WritableStore for Cascading
----------------------------------------
  
First you should upload some input file to HDFS. The input file should reside in /tmp/test.in. E.g. in storehaus-cascading-examples directory do a hadoop fs -put src/main/resources/test.in /tmp/test.in to upload the text-file provided with these examples.

To execute the Cascading job please run ```hadoop jar target/scala-2.10/storehaus-cascading-examples-0.9.1.jar```. 

  * Overall, this is a modified copy-over job from the original Cascading tutorial, step 1. We will also use it for the other examples.

When the job is finished you should be able to run ```cqlsh your-cassandra-ip -k mytestkeyspace -f src/main/resources/example1.cql``` (adapting your-cassandra-ip and mytestkeyspace). This will display the results, which is a table with the contents of test.in.

  * Have a look at the job. You'll notice 3 things:
    * An object called ```SimpleStoreInitializer``` implementing ```StorehausCascadingInitializer[String, String]```. It has a method ```getWritableStore``` which returns a WritableStore from Storehaus. These stores have a ```put``` method.
    * In the job itself there is some ```inTap``` which actually is a ```Hfs[TextDelimited]``` reading plain-text from the file we just uploaded. Thereby it provides all the data for our job.
    * Also, there is some ```outTap`` which is initialized with ```StorehausTap[String, String](SimpleStoreInitializer)```. So this is related to the object with the store. It is a Cascading-Tap sinking all the data from the flow.

Lessons learned: SimpleStoreInitializer (and any other StorehausCascadingInitializer) is used to initialize the WritableStore (or any other store) on client machines. It must be an object, such that the Store can be initialized on Hadoop slaves without actually serializing the store. This allows to use more stores: In ```SimpleStoreInitializer``` you can replace the ```store``` with any WritableStore from Storehaus and the example will still work (please change prepareStore, too, to make sure all resources required by the store get initialized).

Example 2: Creating a source with Storehaus-Cascading
-----------------------------------------------------


