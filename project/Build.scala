/*
 * Copyright 2014 Twitter inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package storehaus

import sbt._
import Keys._
import spray.boilerplate.BoilerplatePlugin.Boilerplate
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact
import sbtassembly.Plugin._
import AssemblyKeys._


object StorehausBuild extends Build {
  def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
    case version if version startsWith "2.10" => "2.10"
    case version if version startsWith "2.11" => "2.11"
    case version if version startsWith "2.12" => "2.12"
    case _ => sys.error("unknown error")
  }
  def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"


  val scalaTestVersion = "2.2.2"
  val scalaCheckVersion = "1.11.5"
  val hadoopVersion = "1.2.1"
  val algebirdVersion = "0.8.2"
  val bijectionVersion = "0.7.0"
  val chillVersion = "0.5.1"
  val slf4jVersion = "1.6.6"
  val parquetVersion = "1.6.0rc4"
  val dfsDatastoresVersion = "1.3.4"
  val scaldingVersion = "0.13.0"
  val storehausVersion = "0.9.1"
  val utilVersion = "6.11.0"
  val finagleVersion = "6.12.2"
  val tormentaVersion = "0.7.0"
  val hbaseVersion = "0.94.10"
  val hravenVersion = "0.9.13"
  val jacksonVersion = "2.4.2"
  val hbaseAsynchBaseVersion = "1.4.1"
  val stumbleuponAsyncVersion = "1.4.0"
  val twitterKafkaVersion = "0.7.0"
  val kafkaVersion = "0.8.0"
  val mongodbCasbahVersion = "2.6.4"
  val elasticsearchVersion = "0.90.9"
  val json4sVersion = "3.2.6"
  val googleFindbugsJsr305Version = "1.3.+"
  val caliperVersion = "0.5-rc1"
  val googleAllocationInstrumenterVersion = "2.0"
  val googleGsonVersion = "1.7.1"
  val specs2Version = "1.13"

  val extraSettings =
    Project.defaultSettings ++ Boilerplate.settings ++ assemblySettings ++ mimaDefaultSettings

  def ciSettings: Seq[Project.Setting[_]] =
    if (sys.env.getOrElse("TRAVIS", "false").toBoolean) Seq(
      ivyLoggingLevel := UpdateLogging.Quiet,
      logLevel in Global := Level.Warn,
      logLevel in Compile := Level.Warn,
      logLevel in Test := Level.Info
    ) else Seq.empty[Project.Setting[_]]

  val testCleanup = Seq(
    testOptions in Test += Tests.Cleanup { loader =>
      val c = loader.loadClass("com.twitter.storehaus.testing.Cleanup$")
      c.getMethod("cleanup").invoke(c.getField("MODULE$").get(c))
    }
  )

  val sharedSettings = extraSettings ++ ciSettings ++ Seq(
    organization := "com.twitter",
    scalaVersion := "2.10.4",
    version := "0.10.0-SNAPSHOT",
    crossScalaVersions := Seq("2.10.4"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    javacOptions in doc := Seq("-source", "1.6"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.specs2" %% "specs2" % specs2Version % "test"
    ),
    resolvers ++= Seq(
      Opts.resolver.sonatypeSnapshots,
      Opts.resolver.sonatypeReleases,
      "Twitter Maven" at "http://maven.twttr.com",
      "Conjars Repository" at "http://conjars.org/repo"
    ),
    parallelExecution in Test := true,
    scalacOptions ++= Seq(Opts.compile.unchecked, Opts.compile.deprecation),

    // Publishing options:
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { x => false },
    publishTo <<= version { v =>
      Some(if (v.trim.toUpperCase.endsWith("SNAPSHOT")) Opts.resolver.sonatypeSnapshots
           else Opts.resolver.sonatypeStaging)
    },
    pomExtra := (
      <url>https://github.com/twitter/storehaus</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/storehaus.git</url>
        <connection>scm:git:git@github.com:twitter/storehaus.git</connection>
      </scm>
      <developers>
        <developer>
          <id>oscar</id>
          <name>Oscar Boykin</name>
          <url>http://twitter.com/posco</url>
        </developer>
        <developer>
          <id>sritchie</id>
          <name>Sam Ritchie</name>
          <url>http://twitter.com/sritchie</url>
        </developer>
      </developers>)
  )

  /**
    * This returns the youngest jar we released that is compatible with
    * the current.
    */
  val unreleasedModules = Set[String]()

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map { s => "com.twitter" % ("storehaus-" + s + "_2.10") % "0.10.0" }


  lazy val storehaus = Project(
    id = "storehaus",
    base = file("."),
    settings = sharedSettings ++ DocGen.publishSettings
    ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(
    storehausCache,
    storehausCore,
    storehausAlgebra,
    storehausMemcache,
    storehausMySQL,
    storehausRedis,
    storehausHBase,
    storehausDynamoDB,
    storehausKafka,
    storehausKafka08,
    storehausMongoDB,
    storehausElastic,
    storehausTesting
  )

  def module(name: String) = {
    val id = "storehaus-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ testCleanup ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    ).dependsOn(storehausTesting % "test->test")
  }

  lazy val storehausCache = module("cache").settings(
    libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "util-core" % utilVersion
  )

  lazy val storehausCore = module("core").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "util-core" % utilVersion % "provided",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-util" % bijectionVersion
    )
  ).dependsOn(storehausCache %  "test->test;compile->compile")

  lazy val storehausAlgebra = module("algebra").settings(
    libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "algebird-util" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "algebird-bijection" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "scalding-date" % scaldingVersion
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausMemcache = module("memcache").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-netty" % bijectionVersion,
      "com.twitter" %% "finagle-memcached" % finagleVersion
    )
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausMySQL = module("mysql").settings(
    libraryDependencies += "com.twitter" %% "finagle-mysql" % finagleVersion
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausRedis = module("redis").settings(
    libraryDependencies ++= Seq (
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-netty" % bijectionVersion,
      "com.twitter" %% "finagle-redis" % finagleVersion
    ),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausHBase= module("hbase").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-hbase" % bijectionVersion ,
      "org.hbase" % "asynchbase" %  hbaseAsynchBaseVersion % "provided->default" intransitive(),
      "com.stumbleupon" % "async" % stumbleuponAsyncVersion % "provided->default" intransitive(),
      "org.apache.hbase" % "hbase" % hbaseVersion % "provided->default" classifier "tests" classifier "",
      "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided->default",
      "org.apache.hadoop" % "hadoop-test" % hadoopVersion % "test"
    ),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausDynamoDB= module("dynamodb").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-hbase" % bijectionVersion ,
      "com.amazonaws" % "aws-java-sdk" % "1.5.7"
      ////use alternator for local testing
      //"com.michelboudreau" % "alternator" % "0.6.4" % "test"
    ),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausKafka = module("kafka").settings(
    libraryDependencies ++= Seq (
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-avro" % bijectionVersion,
      "com.twitter" % "kafka_2.9.2" % twitterKafkaVersion % "provided" excludeAll(
        ExclusionRule("com.sun.jdmk","jmxtools"),
        ExclusionRule( "com.sun.jmx","jmxri"),
        ExclusionRule( "javax.jms","jms")
        )
    ),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausKafka08 = module("kafka-08").settings(
    libraryDependencies ++= Seq (
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-avro" % bijectionVersion,
      "org.apache.kafka" % "kafka_2.9.2" % kafkaVersion % "provided" excludeAll(
        ExclusionRule(organization = "com.sun.jdmk"),
        ExclusionRule(organization = "com.sun.jmx"),
        ExclusionRule(organization = "javax.jms"))
    ),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausCore,storehausAlgebra % "test->test;compile->compile")

  lazy val storehausMongoDB= module("mongodb").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "org.mongodb" %% "casbah" % mongodbCasbahVersion
    ),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausElastic = module("elasticsearch").settings(
    libraryDependencies ++= Seq (
      "org.elasticsearch" % "elasticsearch" % elasticsearchVersion,
      "org.json4s" %% "json4s-native" % json4sVersion,
      "com.google.code.findbugs" % "jsr305" % googleFindbugsJsr305Version,
      "com.twitter" %% "bijection-json4s" % bijectionVersion
    ),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")


  val storehausTesting = Project(
    id = "storehaus-testing",
    base = file("storehaus-testing"),
    settings = sharedSettings ++ Seq(
      name := "storehaus-testing",
      previousArtifact := youngestForwardCompatible("testing"),
      libraryDependencies ++= Seq(
        "org.scalacheck" %% "scalacheck" % scalaCheckVersion withSources(),
        "com.twitter" %% "util-core" % utilVersion
      )
    )
  )

  lazy val storehausCaliper = module("caliper").settings(
    libraryDependencies ++= Seq("com.google.caliper" % "caliper" % caliperVersion,
      "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % googleAllocationInstrumenterVersion,
      "com.google.code.gson" % "gson" % googleGsonVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion),
      javaOptions in run <++= (fullClasspath in Runtime) map { cp => Seq("-cp", sbt.Build.data(cp).mkString(":")) }
  ).dependsOn(storehausCore, storehausAlgebra, storehausCache)

}
