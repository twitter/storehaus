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
  def withCross(dep: ModuleID) =
    dep cross CrossVersion.binaryMapped {
      case version if version startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
      case x => x
    }

  def bogus210Version(dep: ModuleID) = dep cross CrossVersion.binaryMapped {
    case version if version startsWith "2.10" => "2.10.3"
    case x => x
  }

  def isScalaGreater29x(scalaVersion: String) = scalaVersion match {
    case version if version startsWith "2.9" => false
    case version if version matches "^2.1[0-2].*" => true
  }

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
    scalaVersion := "2.10.5",
    crossScalaVersions := Seq("2.10.5", "2.11.7"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    javacOptions in doc := Seq("-source", "1.6"),
    libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    resolvers ++= Seq(
      Opts.resolver.sonatypeSnapshots,
      Opts.resolver.sonatypeReleases,
      "Conjars Repository" at "http://conjars.org/repo"
//      "Bintray JCenter" at "https://jcenter.bintray.com/",
//      "Twitter Maven" at "http://maven.twttr.com",
//      "Conjars Repository" at "http://conjars.org/repo",
//      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
//      "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
//      "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
    ),
    parallelExecution in Test := true,
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-Xlint",
      "-Yresolve-term-conflict:package"
    ),

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
      .map { s => "com.twitter" % ("storehaus-" + s + "_2.10") % "0.12.0" }

  val algebirdVersion = "0.11.0"
  val bijectionVersion = "0.8.0"
  val utilVersion = "6.26.0"
  val scaldingVersion = "0.15.0"
  val finagleVersion = "6.27.0"
  val scalatestVersion = "2.2.4"
  val cascadingVersion = "2.5.2"
  val hadoopVersion = "1.2.1"
  val cassandraDriverVersion = "2.1.7.1"
  val cassandraVersion = "2.1.3"

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
    storehausLevelDB,
    storehausKafka08,
    storehausMongoDB,
    storehausElastic,
    storehausCassandra,
    storehausCascading,
    storehausCascadingExamples,
    storehausHttp,
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
    libraryDependencies += withCross("com.twitter" %% "util-core" % utilVersion)
  )

  lazy val storehausCore = module("core").settings(
    libraryDependencies ++= Seq(
      withCross("com.twitter" %% "util-core" % utilVersion % "provided"),
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
      "com.twitter" %% "finagle-memcachedx" % finagleVersion excludeAll(
        // we don't use this and its not on maven central.
        ExclusionRule("com.twitter.common.zookeeper"),
        ExclusionRule("com.twitter.common")
        )
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

  lazy val storehausHBase = module("hbase").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-hbase" % bijectionVersion ,
      "org.hbase" % "asynchbase" % "1.4.1" % "provided->default" intransitive(),
      "com.stumbleupon" % "async" % "1.4.0" % "provided->default" intransitive(),
      "org.apache.hbase" % "hbase" % "0.94.6" % "provided->default" classifier "tests" classifier "",
      "org.apache.hadoop" % "hadoop-core" % "1.2.0" % "provided->default",
      "org.apache.hadoop" % "hadoop-test" % "1.2.0" % "test"
    ),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausDynamoDB = module("dynamodb").settings(
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

  lazy val storehausLevelDB = module("leveldb").settings(
    libraryDependencies +=
      "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
    parallelExecution in Test := false,
    // workaround because of how sbt handles native libraries
    // http://stackoverflow.com/questions/19425613/unsatisfiedlinkerror-with-native-library-under-sbt
    testOptions in Test := Seq(),
    fork in Test := true
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausKafka08 = module("kafka-08").settings(
    libraryDependencies ++= Seq (
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-avro" % bijectionVersion,
      "org.apache.kafka" %% "kafka" % "0.8.2.1" % "provided" excludeAll(
        ExclusionRule(organization = "com.sun.jdmk"),
        ExclusionRule(organization = "com.sun.jmx"),
        ExclusionRule(organization = "javax.jms"))
    ),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausCore, storehausAlgebra % "test->test;compile->compile")

  lazy val storehausMongoDB = module("mongodb").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "org.mongodb" %% "casbah" % "2.8.2"
    ),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausElastic = module("elasticsearch").settings(
    libraryDependencies ++= Seq (
      "org.elasticsearch" % "elasticsearch" % "0.90.9",
      "org.json4s" %% "json4s-native" % "3.2.10",
      "com.google.code.findbugs" % "jsr305" % "1.3.+",
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
        "org.scalacheck" %% "scalacheck" % "1.12.2" withSources(),
        withCross("com.twitter" %% "util-core" % utilVersion)
      )
    )
  )

  lazy val storehausCaliper = module("caliper").settings(
    libraryDependencies ++= Seq("com.google.caliper" % "caliper" % "0.5-rc1",
      "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % "2.0",
      "com.google.code.gson" % "gson" % "1.7.1",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion
    ),
    javaOptions in run <++= (fullClasspath in Runtime) map { cp => Seq("-cp", sbt.Build.data(cp).mkString(":")) }
  ).dependsOn(storehausCore, storehausAlgebra, storehausCache)

  def cassandraDeps(scalaVersion: String) = if (!isScalaGreater29x(scalaVersion)) Seq() else Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.google.code.findbugs" % "jsr305" % "1.3.+",
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion classifier "shaded" exclude("io.netty", "*"),
    "org.apache.cassandra" % "cassandra-thrift" % cassandraVersion exclude ("com.google.guava", "guava"),
    "org.apache.cassandra" % "cassandra-all" % cassandraVersion exclude ("com.google.guava", "guava"),
    "com.websudos" %% "phantom-dsl" % "1.5.0" exclude ("com.datastax.cassandra", "cassandra-driver-core"),
    withCross("com.twitter" %% "util-zk" % utilVersion)  exclude ("com.google.guava", "guava"),
    bogus210Version("com.chuusai" %% "shapeless" % "2.0.0"),
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion
  )

  lazy val storehausCassandra = module("cassandra").settings(
    skip in test := !isScalaGreater29x(scalaVersion.value),
    skip in compile := !isScalaGreater29x(scalaVersion.value),
    skip in doc := !isScalaGreater29x(scalaVersion.value),
    publishArtifact := isScalaGreater29x(scalaVersion.value),
    libraryDependencies ++= cassandraDeps(scalaVersion.value),
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile", storehausCascading)

  def cascadingDeps(scalaVersion: String) = if (!isScalaGreater29x(scalaVersion)) Seq() else Seq(
    "cascading" % "cascading-core" % cascadingVersion,
    "cascading" % "cascading-hadoop" % cascadingVersion,
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "org.scala-lang" % "scala-reflect" % scalaVersion,
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion
  )

  lazy val storehausCascading = module("cascading").settings(
    skip in test := !isScalaGreater29x(scalaVersion.value),
    skip in compile := !isScalaGreater29x(scalaVersion.value),
    skip in doc := !isScalaGreater29x(scalaVersion.value),
    publishArtifact := isScalaGreater29x(scalaVersion.value),
    libraryDependencies ++= cascadingDeps(scalaVersion.value),
    parallelExecution in Test := false
  ).dependsOn(storehausCore, storehausAlgebra % "test->test;compile->compile")

  def cascadingExampleDeps(scalaVersion: String) = if (!isScalaGreater29x(scalaVersion)) Seq() else Seq(
    "cascading" % "cascading-core" % cascadingVersion,
    "cascading" % "cascading-hadoop" % cascadingVersion,
    "org.slf4j" % "slf4j-api" % "1.7.5",
    bogus210Version("com.chuusai" %% "shapeless" % "2.0.0"),
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion,
    "com.twitter" % "chill-hadoop" % "0.5.1"
  )

  lazy val storehausCascadingExamples = module("cascading-examples").settings(
    skip in test := !isScalaGreater29x(scalaVersion.value),
    skip in compile := !isScalaGreater29x(scalaVersion.value),
    skip in doc := !isScalaGreater29x(scalaVersion.value),
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
        case x if x startsWith "javax/servlet" => MergeStrategy.first
        case x if x startsWith "javax/xml/stream/" => MergeStrategy.last
        case x if x startsWith "org/apache/commons/beanutils/" => MergeStrategy.last
        case x if x startsWith "org/apache/commons/collections/" => MergeStrategy.last
        case x if x startsWith "org/apache/jasper/" => MergeStrategy.last
        case x if x startsWith "com/twitter/common/args/apt/" => MergeStrategy.last
        case x if x startsWith "org/slf4j/" => MergeStrategy.first
        case x if x startsWith "com/esotericsoftware/minlog" => MergeStrategy.first
        case x if x startsWith "org/objectweb/asm" => MergeStrategy.first
        case x if x startsWith "org/stringtemplate/v4" => MergeStrategy.first
        case x => old(x)
      }
    },
    jarName in assembly := s"${name.value}-${version.value}.jar",
    libraryDependencies ++= cascadingExampleDeps(scalaVersion.value),
    publishArtifact := false,
    parallelExecution in Test := false
  ).dependsOn(storehausCore, storehausAlgebra % "test->test;compile->compile", storehausCascading, storehausCassandra, storehausMemcache)

  lazy val storehausHttp = module("http").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "finagle-http" % finagleVersion,
      "com.twitter" %% "bijection-netty" % bijectionVersion
    )
  ).dependsOn(storehausCore)
}
