package com.twitter.storehaus.cassandra.cql.cascading

import com.twitter.storehaus.cascading.StorehausOutputFormat.OutputThrottler
import com.twitter.util.{Duration, JavaTimer, Try}
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import javax.management.ObjectName
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import com.twitter.storehaus.cascading.StorehausInputFormat
import com.twitter.util.Return
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import java.net.UnknownHostException
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.QueryOptions
import java.util.{Map => JMap, HashMap => JHashMap}

/**
 * A self-adapting throttler for Cassandra. If used with StorehausOutputFormat it can
 * self-adapt to Cassandra starting to queue up tasks. It will use the maximum number of
 * tasks in any of Cassandra's mutation-queue to determine a throttle delay in ms, if the
 * number of pending tasks exceeds 
 * com.twitter.storehaus.cassandra.cql.cascading.throttler.request.min.tasks 
 * 
 * Use com.twitter.storehaus.cassandra.cql.cascading.throttler.request.delay.seconds in 
 * JobConf to adjust an appropriate polling intervals in seconds.
 * 
 * It is required to either set
 * com.twitter.storehaus.cassandra.cql.cascading.throttler.request.main.host
 * or to use CassandraSplittingMechanism
 */
class CassandraSelfAdaptingThrottler extends OutputThrottler {
  
  @transient val logger = LoggerFactory.getLogger(classOf[CassandraSelfAdaptingThrottler])
  
  @volatile var throttleDelayMS = 0L
  
  val CASSANDRA_REQUEST_REPEAT_DELAY = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.delay.seconds"
  val CASSANDRA_JMX_PORT = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.jmxport"
  val CASSANDRA_REQUEST_MINIMUM_TASKS = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.min.tasks"
  val CASSANDRA_MAIN_HOST = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.host"

  val timer = new JavaTimer(true)

  def configure(conf: JobConf) = {
    val seconds = Try(Option(conf.get(CASSANDRA_REQUEST_REPEAT_DELAY)).getOrElse("30").toInt).getOrElse(30)
    val jmxport = Try(Option(conf.get(CASSANDRA_JMX_PORT)).getOrElse("7199").toInt).getOrElse(7199)
    val minTaks = Try(Option(conf.get(CASSANDRA_REQUEST_MINIMUM_TASKS)).getOrElse("50").toLong).getOrElse(50L)
    
    // get a Cassandra-host in some way 
    val cassandraHost: Option[Array[String]] = Option(conf.get(CASSANDRA_MAIN_HOST)) match {
      case Some(host) => Some(Array(host))
      case None => StorehausInputFormat.getSplittingClass(conf).toOption.flatMap { smech: StorehausSplittingMechanism[_, _, _] => 
        if(smech.isInstanceOf[CassandraSplittingMechanism[_, _, _]]) {
          Some(smech.asInstanceOf[CassandraSplittingMechanism[_, _, _]].storeinit.getThriftConnections.trim.split(",")(0).split(":").map(s => s.trim))
        } else {
          logger.warn(s"please either provide CassandraSplittingMechanism or com.twitter.storehaus.cassandra.cql.cascading.throttler.request.host in JobConf.")
          None
        }
      }
    }
    
    cassandraHost.map { hosts => 
      timer.schedule(Duration.fromSeconds(seconds)) {
        val hostset = hosts.foldLeft(Set[String]()){(current, possiblehost) => 
          if(current.size == 0) {
            JMXConnector.fetchAllHosts(possiblehost, jmxport).getOrElse(current)
          } else {
            current
          }
        }
        val maxms = hostset.foldLeft(0L) { (value, host) => 
          val hostPendingTasks = JMXConnector.requestObject[Long](JMXConnector.createJMXURL(host, jmxport), 
                  "org.apache.cassandra.request:type=MutationStage", "PendingTasks", 0L).onFailure { e =>
              logger.info(s"Cannot access $host via JMX to retrieve information about pending tasks: ${e.getLocalizedMessage}") }.
              getOrElse(0L)
          (hostPendingTasks > value) match {
            case true => hostPendingTasks
            case false => value
          } 
        }
        if(maxms >= minTaks) {
          if(maxms != throttleDelayMS) {
            logger.info(s"Queues on the Cassandra cluster seem to fill up. Setting throttle delay to queue length, i.e. $maxms ms.")
          }
          throttleDelayMS = maxms
        } else {
          if(throttleDelayMS != 0L) {
            logger.info(s"Queues on the Cassandra cluster seem to be empty. Disabling throttle delay.")
          }
          throttleDelayMS = 0L
        }
      }
    }
  }
  
  def close = timer.stop()
  
  /**
   * throttle speed by sleeping
   */
  def throttle = throttleDelayMS match {
    case delay if delay > 0 => Thread.sleep(delay)
    case _ =>
  }
}

object JMXConnector {
  
  def createJMXURL(host: String, port: Int) = s"service:jmx:rmi:///jndi/rmi://$host:$port/jmxrmi"
  
  def requestObject[T](url: String, objectCoordinate: String, attribute: String, default: T): Try[T] = Try {
    val jmxcon = JMXConnectorFactory.connect(new JMXServiceURL(url), null)
    val mbsc = jmxcon.getMBeanServerConnection()
    val mbeanName = new ObjectName(objectCoordinate)
    val result = mbsc.getAttribute(mbeanName, attribute)
    jmxcon.close()
    Try(result.asInstanceOf[T]).getOrElse(default)
  }  

  def fetchAllHosts(host: String, port: Int): Try[Set[String]] = {
    import scala.collection.convert.WrapAsScala._
    val hostmap = JMXConnector.requestObject[JMap[String, String]](
            JMXConnector.createJMXURL(host, port), "org.apache.cassandra.net:type=FailureDetector", 
            "SimpleStates", new JHashMap[String, String]())
    hostmap.map { hosts => hosts.filter(entry => entry._2 == "UP").map(host => host._1.stripPrefix("/")).toSet } 
  }
}
