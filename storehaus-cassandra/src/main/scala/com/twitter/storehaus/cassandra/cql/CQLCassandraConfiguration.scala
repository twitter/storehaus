/*
 * Copyright 2014 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.storehaus.cassandra.cql

import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, Cluster, Session}
import com.datastax.driver.core.policies.{LoadBalancingPolicy, Policies, ReconnectionPolicy, RetryPolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit

object CQLCassandraConfiguration {

  val DEFAULT_VALUE_COLUMN_NAME = "value"
  val DEFAULT_KEY_COLUMN_NAME = "key"
  val DEFAULT_TOKEN_COLUMN_NAME = "tokencol"
  val DEFAULT_FUTURE_POOL_SIZE = 10
  val DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM
  val DEFAULT_TTL_DURATION = None
  val DEFAULT_BATCH_STATEMENT_TYPE = BatchStatement.Type.UNLOGGED
  val DEFAULT_SYNC = CassandraExternalSync(NoSync(), NoSync())
  val DEFAULT_SHUTDOWN_TIMEOUT = Duration(60, TimeUnit.SECONDS)
  
  case class StoreHost(val name: String)

  case class StoreCredentials(val user: String, val pwd: String)

  case class StoreCluster(val name: String, val hosts: Set[StoreHost],
	val credentials: Option[StoreCredentials] = None,
	val loadBalancing: LoadBalancingPolicy = Policies.defaultLoadBalancingPolicy,
	val reconnectPolicy: ReconnectionPolicy = Policies.defaultReconnectionPolicy,
	val retryPolicy: RetryPolicy = Policies.defaultRetryPolicy,
	val shutdownTimeout: Duration = DEFAULT_SHUTDOWN_TIMEOUT) {
	def getCluster: Cluster = {
	  val clusterBuilder = Cluster.builder()
	  hosts.foreach(host => {
	    val hostPort = host.name.split(":")
	    hostPort.length match {
	      case 2 => {
	        clusterBuilder.addContactPoint(hostPort.apply(0)).withPort(hostPort.apply(1).toInt)
	      }
	      case _ => clusterBuilder.addContactPoint(host.name)
	    }      
	  })
	  credentials.map(cred => clusterBuilder.withCredentials(cred.user, cred.pwd))
	  clusterBuilder.withLoadBalancingPolicy(loadBalancing)
	  clusterBuilder.withReconnectionPolicy(reconnectPolicy)
	  clusterBuilder.withRetryPolicy(retryPolicy)
	  clusterBuilder.build()
	} 
  }
		
  case class StoreColumnFamily(name: String, val session: StoreSession) {
    lazy val getName = name.filterNot(_ == '"')
    lazy val getPreparedNamed = {
      val lowerName = getName.toLowerCase
      if (getName != lowerName)
        "\"" + getName + "\""
      else
        getName
    } 
  }
  
  case class StoreSession(val keyspacename: String, val cluster: StoreCluster, val replicationOptions: String = "{'class' : 'SimpleStrategy', 'replication_factor' : 3}") {
	lazy val session = cluster.getCluster.connect("\"" + getKeyspacename + "\"")
	def getSession: Session = session
	def getCluster: Cluster = getSession.getCluster
	def getKeyspacename : String = keyspacename.filterNot(_ == '"')
    def createKeyspace = {
	  val stmt = "CREATE KEYSPACE IF NOT EXISTS \"" + getKeyspacename + "\" WITH REPLICATION = " + replicationOptions + " ;"
	  independentKeyspaceOp(stmt)
	}
	/**
	 * WARNING! *Permanently DELETES* given keyspace and contained data. Session may not be used afterwards.
	 */
    def dropAndDeleteKeyspaceAndContainedData = {
	  val stmt = "DROP KEYSPACE \"" + getKeyspacename + "\";"
	  independentKeyspaceOp(stmt)
	}
    private def independentKeyspaceOp(stmt: String) = {
	  val tmpSession = cluster.getCluster.connect() 
	  tmpSession.execute(stmt)
	  tmpSession.close()      
    }
  }
}
