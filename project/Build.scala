import sbt._
import Keys._

object StorehausBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ Seq(
    organization := "com.twitter",
    version := "0.0.3-SNAPSHOT",
    scalaVersion := "2.9.2",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test" withSources(),
      "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9" % "test" withSources()
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
      val nexus = "http://artifactory.local.twitter.com/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "libs-snapshots-local")
      else
        Some("sonatype-releases"  at nexus + "libs-releases-local")
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
          <id>oscar</id>
          <name>Marius Eriksen</name>
          <url>http://twitter.com/marius</url>
        </developer>
        <developer>
          <id>sritchie</id>
          <name>Sam Ritchie</name>
          <url>http://twitter.com/sritchie</url>
        </developer>
      </developers>)
  )

  lazy val storehaus = Project(
    id = "storehaus",
    base = file(".")
    ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(storehausCore,
              storehausAlgebra,
              storehausMemcache)

  lazy val storehausCore = Project(
    id = "storehaus-core",
    base = file("storehaus-core"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-core",
    libraryDependencies ++= Seq("com.twitter" % "util-core" % "5.3.15",
      "com.twitter" %% "bijection-core" % "0.2.0")
  )

  lazy val storehausAlgebra = Project(
    id = "storehaus-algebra",
    base = file("storehaus-algebra"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-algebra",
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird" % "0.1.6",
      "com.twitter" %% "bijection-core" % "0.2.0"
    )
  ).dependsOn(storehausCore % "test->test;compile->compile")

  lazy val storehausMemcache = Project(
    id = "storehaus-memcache",
    base = file("storehaus-memcache"),
    settings = sharedSettings
  ).settings(
    name := "storehaus-memcache",
    libraryDependencies ++= Seq(
      "com.twitter" %% "bijection-core" % "0.2.0",
      "com.twitter" % "finagle-memcached" % "5.3.23"
    )
  ).dependsOn(storehausCore % "test->test;compile->compile")
}
