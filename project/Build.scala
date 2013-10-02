/*
 * Copyright 2013 Twitter inc.
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
import sbtgitflow.ReleasePlugin._
import spray.boilerplate.BoilerplatePlugin.Boilerplate
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object StorehausBuild extends Build {
  def withCross(dep: ModuleID) =
    dep cross CrossVersion.binaryMapped {
      case "2.9.3" => "2.9.2" // TODO: hack because twitter hasn't built things against 2.9.3
      case version if version startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
      case x => x
    }

  val extraSettings =
    Project.defaultSettings ++ releaseSettings ++ Boilerplate.settings ++ mimaDefaultSettings

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
    scalaVersion := "2.9.3",
    crossScalaVersions := Seq("2.9.3", "2.10.0"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    javacOptions in doc := Seq("-source", "1.6"),
    libraryDependencies += "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" withSources(),
    resolvers ++= Seq(
      Opts.resolver.sonatypeSnapshots,
      Opts.resolver.sonatypeReleases,
      "Twitter Maven" at "http://maven.twttr.com"
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
      .map { s => "com.twitter" % ("storehaus-" + s + "_2.9.3") % "0.5.0" }

  val algebirdVersion = "0.2.0"
  val bijectionVersion = "0.5.3"

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
    storehausTesting
  )

  def module(name: String) = {
    val id = "storehaus-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ testCleanup ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    ).dependsOn(storehausTesting % "test->test")
  }

  lazy val storehausCache = module("cache")

  lazy val storehausCore = module("core").settings(
    libraryDependencies ++= Seq(
      withCross("com.twitter" %% "util-core" % "6.3.7"),
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-util" % bijectionVersion
    )
  ).dependsOn(storehausCache %  "test->test;compile->compile")

  lazy val storehausAlgebra = module("algebra").settings(
    libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "algebird-util" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "algebird-bijection" % algebirdVersion
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausMemcache = module("memcache").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-netty" % bijectionVersion,
      Finagle.module("memcached")
    )
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausMySQL = module("mysql").settings(
    libraryDependencies += Finagle.module("mysql")
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausRedis = module("redis").settings(
    libraryDependencies += Finagle.module("redis"),
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausHBase= module("hbase").settings(
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

  val storehausTesting = Project(
    id = "storehaus-testing",
    base = file("storehaus-testing"),
    settings = sharedSettings ++ Seq(
      name := "storehaus-testing",
      previousArtifact := youngestForwardCompatible("testing"),
      libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.0" withSources()
    )
  )
}
