package storehaus

import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._
import spray.boilerplate.BoilerplatePlugin.Boilerplate
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object StorehausBuild extends Build {
  val extraSettings =
    Project.defaultSettings ++ releaseSettings ++ Boilerplate.settings ++ mimaDefaultSettings
  val sharedSettings =  extraSettings ++ Seq(
    organization := "com.twitter",
    crossScalaVersions := Seq("2.9.2", "2.10.0"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources(),
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" withSources()
    ),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Twitter Maven" at "http://maven.twttr.com"
    ),

    parallelExecution in Test := true,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-releases"  at nexus + "service/local/staging/deploy/maven2")
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
    *
    * TODO: Remove mysql, redis and cache after their release.
    */
  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(Set("mysql", "redis", "cache").contains(_))
      .map { s => "com.twitter" % ("storehaus-" + s + "_2.9.2") % "0.2.0" }

  val algebirdVersion = "0.1.12"
  val bijectionVersion = "0.3.0"

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
    storehausRedis
  )

  lazy val storehausCache = Project(
    id = "storehaus-cache",
    base = file("storehaus-cache"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-cache",
    previousArtifact := youngestForwardCompatible("cache")
  )

  lazy val storehausCore = Project(
    id = "storehaus-core",
    base = file("storehaus-core"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-core",
    previousArtifact := youngestForwardCompatible("core"),
    libraryDependencies += "com.twitter" %% "util-core" % "6.3.0"
  ).dependsOn(storehausCache)

  lazy val storehausAlgebra = Project(
    id = "storehaus-algebra",
    base = file("storehaus-algebra"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-algebra",
    previousArtifact := youngestForwardCompatible("algebra"),
    libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "algebird-util" % algebirdVersion,
    libraryDependencies += "com.twitter" %% "bijection-core" % bijectionVersion,
    libraryDependencies += "com.twitter" %% "bijection-algebird" % bijectionVersion
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausMemcache = Project(
    id = "storehaus-memcache",
    base = file("storehaus-memcache"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-memcache",
    previousArtifact := youngestForwardCompatible("memcache"),
    libraryDependencies += "com.twitter" %% "finagle-memcached" % "6.3.0"
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")

  lazy val storehausMySQL = Project(
    id = "storehaus-mysql",
    base = file("storehaus-mysql"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-mysql",
    previousArtifact := youngestForwardCompatible("mysql"),
    libraryDependencies += "com.twitter" %% "finagle-mysql" % "6.2.1"
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausRedis = Project(
    id = "storehaus-redis",
    base = file("storehaus-redis"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-redis",
    previousArtifact := youngestForwardCompatible("redis"),
    libraryDependencies += "com.twitter" %% "finagle-redis" % "6.2.0",
    // we don't want various tests clobbering each others keys
    parallelExecution in Test := false,
    testOptions in Test += Tests.Cleanup { loader =>
      val c = loader.loadClass("com.twitter.storehaus.redis.Cleanup$")
      c.getMethod("cleanup").invoke(c.getField("MODULE$").get(c))
    }
  ).dependsOn(storehausAlgebra % "test->test;compile->compile")
}
