package com.twitter.storehaus.cassandra.cql.cascading

import java.util.{ HashMap => JHashMap }
import java.util.{ Map => JMap }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import com.twitter.storehaus.WritableStore
import com.twitter.storehaus.cascading.StorehausOutputFormat.OutputThrottler
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import com.twitter.storehaus.cassandra.cql.CassandraTupleMultiValueStore
import com.twitter.storehaus.cassandra.cql.CassandraTupleStore
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import com.twitter.util.Try
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL
import scala.collection.mutable.ArrayBuffer
import com.twitter.util.RingBuffer

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

  val effectiveThrottleDelays: ArrayBuffer[Long] = ArrayBuffer[Long](0, 0)

  val timer = new JavaTimer(true)

  override def configure[K, V](conf: JobConf, store: WritableStore[K, Option[V]]) = {
    import CassandraSelfAdaptingThrottler._

    logger.info(s"Using store")

    val colfam = store match {
      case acqlstore: AbstractCQLCassandraStore[_, _]                              => acqlstore.columnFamily
      case ctuplestore: CassandraTupleStore[_, _, _, _, _, _, _]                   => ctuplestore.store.columnFamily
      case ctuplemvstore: CassandraTupleMultiValueStore[_, _, _, _, _, _, _, _, _] => ctuplemvstore.store.columnFamily
    }

    logger.info(s"Reading variables")

    val seconds = Try(Option(conf.get(CASSANDRA_REQUEST_REPEAT_DELAY)).getOrElse("30").toInt).getOrElse(30)
    val jmxport = Try(Option(conf.get(CASSANDRA_JMX_PORT)).getOrElse("7199").toInt).getOrElse(7199)
    val minTaks = Try(Option(conf.get(CASSANDRA_REQUEST_MINIMUM_TASKS)).getOrElse("50").toLong).getOrElse(50L)
    val exponent = Try(Option(conf.get(CASSANDRA_REQUEST_THROTTLE_EXP)).getOrElse("2").toDouble).getOrElse(2D)
    val hist = Try(Option(conf.get(CASSANDRA_REQUEST_DELAY_HIST)).getOrElse("5").toInt).getOrElse(5)

    val buffer = new RingBuffer[Long](hist / seconds)
    fillBuffer(0L, buffer)

    logger.info(s"Initializing ${classOf[CassandraSelfAdaptingThrottler]} with $seconds repeat time and jmx port $jmxport with a threshold of ${minTaks}")

    // get a Cassandra-host in some way 
    val cassandraHost: Option[Array[String]] = Option(conf.get(CASSANDRA_MAIN_HOST)) match {
      case Some(host) => Some(Array(host))
      case None       => Some(colfam.session.cluster.hosts.map(host => host.name.split(":")(0)).toArray)
    }

    logger.info(s"Using host initial host $cassandraHost")

    cassandraHost.map { hosts =>
      timer.schedule(Duration.fromSeconds(seconds)) {
        logger.info(s"Scheduling run")
        val hostset = hosts.foldLeft(Set[String]()) { (current, possiblehost) =>
          if (current.size == 0) {
            JMXConnector.fetchAllHosts(possiblehost, jmxport).getOrElse(current)
          } else {
            current
          }
        }
        logger.info(s"Hostset is $hostset")
        val maxms = hostset.foldLeft(0L) { (value, host) =>
          val hostPendingTasks = JMXConnector.requestObject[Long](JMXConnector.createJMXURL(host, jmxport),
            "org.apache.cassandra.request:type=MutationStage", "PendingTasks", 0L).onFailure { e =>
              logger.info(s"Cannot access $host via JMX to retrieve information about pending tasks: ${e.getLocalizedMessage}")
            }.
            getOrElse(0L)
          (hostPendingTasks > value) match {
            case true  => hostPendingTasks
            case false => value
          }
        }

        logger.info(s"Pending tasks count is $maxms")

        if (maxms >= minTaks && maxms > throttleDelayMS) {
          logger.info(s"Queues on the Cassandra cluster seem to fill up. Setting throttle delay to queue length, i.e. $maxms ms.")
          fillBuffer(maxms, buffer)
        }

        val dampenedThrottleDelayMS = calculateSumOfExps(buffer, exponent, maxms)

        if (dampenedThrottleDelayMS < minTaks) {
          if (dampenedThrottleDelayMS != throttleDelayMS) {
            logger.info(s"Queues on the Cassandra cluster have sufficiently shrinked. Disabling throttle delay.")
          }
          throttleDelayMS = 0L
        } else {
          if (dampenedThrottleDelayMS != throttleDelayMS) {
            logger.info(s"Adjusting throttle delay to $throttleDelayMS ms.")
          }
          throttleDelayMS = dampenedThrottleDelayMS.toLong
        }

        buffer += maxms

        val delaySoFar = effectiveThrottleDelays.reduce((a, b) => a + b) / 1000000
        logger.info(s"Effective throttle delay so far has been $delaySoFar milis.")
      }
    }
  }

  def fillBuffer[T](value: T, buffer: RingBuffer[T]) = {
    1 to buffer.maxSize foreach (_ => buffer += value)
  }

  def calculateSumOfExps(buffer: RingBuffer[Long], exponent: Double, addValue: Long, fract: Double = 1.0d) = {
    val normalization: Double = 1 / (1 to buffer.size + 1).foldLeft(0d)((old, index) => old + math.pow(index.toDouble, exponent))
    (2 to buffer.size + 1).map { index: Int =>
      math.pow(index.toDouble, exponent) * buffer((buffer.size - index + 1)) * normalization
    }.reduce(_ + _) + addValue * normalization
  }

  def close = timer.stop()

  /**
   * throttle speed by sleeping
   */
  def throttle = if (throttleDelayMS > 0) {
    val before = System.nanoTime()
    try {
      Thread.sleep(throttleDelayMS)
    } catch {
      case ex: InterruptedException => logger.warn("Being interrupted.")
    }
    effectiveThrottleDelays += (System.nanoTime() - before)
  }
}

object CassandraSelfAdaptingThrottler {
  val CASSANDRA_JMX_PORT = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.jmxport"
  val CASSANDRA_MAIN_HOST = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.host"
  val CASSANDRA_REQUEST_MINIMUM_TASKS = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.min.tasks"
  val CASSANDRA_REQUEST_REPEAT_DELAY = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.delay.seconds"
  val CASSANDRA_REQUEST_DELAY_HIST = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.hist.seconds"
  val CASSANDRA_REQUEST_THROTTLE_EXP = "com.twitter.storehaus.cassandra.cql.cascading.throttler.request.throttle.exp"
}

object JMXConnector {

  @transient val logger = LoggerFactory.getLogger(JMXConnector.getClass.getName)

  def createJMXURL(host: String, port: Int) = s"service:jmx:rmi:///jndi/rmi://$host:$port/jmxrmi"

  def requestObject[T](url: String, objectCoordinate: String, attribute: String, default: T): Try[T] = Try {
    val jmxcon = JMXConnectorFactory.connect(new JMXServiceURL(url), null)
    val mbsc = jmxcon.getMBeanServerConnection()
    val mbeanName = new ObjectName(objectCoordinate)
    val result = mbsc.getAttribute(mbeanName, attribute)
    logger.info(s"requestObject returns $result")
    jmxcon.close()
    Try(result.asInstanceOf[T]).getOrElse(default)
  }

  def fetchAllHosts(host: String, port: Int): Try[Set[String]] = {
    import scala.collection.convert.WrapAsScala._
    val hostmap = JMXConnector.requestObject[JMap[String, String]](
      JMXConnector.createJMXURL(host, port), "org.apache.cassandra.net:type=FailureDetector",
      "SimpleStates", new JHashMap[String, String]())
    logger.info(s"fetchAllHosts returns $hostmap")
    hostmap.map { hosts => hosts.filter(entry => entry._2 == "UP").map(host => host._1.stripPrefix("/")).toSet }
  }
}
