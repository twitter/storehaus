package com.twitter.storehaus.cassandra

import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.hector.api.{ Keyspace, Cluster, ConsistencyLevelPolicy, HConsistencyLevel }
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.{ QuorumAllConsistencyLevelPolicy, ConfigurableConsistencyLevel }

object CassandraConfiguration {

  // some defaults
  val DEFAULT_CONSISTENCY_LEVEL = new QuorumAllConsistencyLevelPolicy()
  val DEFAULT_VALUE_COLUMN_NAME = "value"
  val DEFAULT_FUTURE_POOL_SIZE = 10
  val DEFAULT_COUNTER_COLUMN_CONSISTENCY_LEVEL = getConsistencyLevelRONEWONE
  val DEFAULT_TTL_DURATION = None
  val DEFAULT_SYNC = new CassandraLongSync(new NoSync, new NoSync)
  val DEFAULT_SERIALIZER_LIST = ScalaSerializables.serializerList

  def getHostConfigurator(
    hostNames: StoreHost,
    autoDiscoverHosts: Boolean = true,
    autoDiscoverAtStartup: Boolean = true,
    retryDownedHosts: Boolean = true): CassandraHostConfigurator = {
    val cassHostConfigurator = new CassandraHostConfigurator(hostNames.name)
    cassHostConfigurator.setRunAutoDiscoveryAtStartup(autoDiscoverAtStartup)
    cassHostConfigurator.setAutoDiscoverHosts(autoDiscoverHosts)
    cassHostConfigurator.setRetryDownedHosts(retryDownedHosts)
    cassHostConfigurator
  }

  def getConsistencyLevel(reads: HConsistencyLevel, writes: HConsistencyLevel) = {
    val policy = new ConfigurableConsistencyLevel()
    policy.setDefaultReadConsistencyLevel(reads)
    policy.setDefaultWriteConsistencyLevel(writes)
    policy
  }

  def getConsistencyLevelRONEWONE(): ConsistencyLevelPolicy = {
    getConsistencyLevel(HConsistencyLevel.ONE, HConsistencyLevel.ONE)
  }

  def getConsistencyLevelRALLWONE(): ConsistencyLevelPolicy = {
    getConsistencyLevel(HConsistencyLevel.ALL, HConsistencyLevel.ONE)
  }

  case class StoreHost(val name: String)

  case class StoreCluster(val name: String, val host: StoreHost,
    val autoDiscoverAtStartup: Boolean = true,
    val autoDiscoverHosts: Boolean = true,
    val retryDownedHosts: Boolean = true) {
    def getCluster: Cluster = HFactory.getOrCreateCluster(name,
      CassandraConfiguration.getHostConfigurator(host, autoDiscoverHosts, autoDiscoverAtStartup, retryDownedHosts))
    def getHost: String = host.name
  }

  case class StoreKeyspace(val name: String, val cluster: StoreCluster) {
    val keyspace = HFactory.createKeyspace(name, getCluster)
    def getKeyspace: Keyspace = keyspace
    def getCluster: Cluster = cluster.getCluster
    def getHost: String = cluster.getHost
  }

  case class StoreColumnFamily(val name: String)

}

