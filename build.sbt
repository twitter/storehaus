import ReleaseTransformations._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

def withCross(dep: ModuleID) =
  dep cross CrossVersion.binaryMapped {
    case ver if ver startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
    case x => x
  }

val extraSettings =
  Boilerplate.settings ++ assemblySettings ++ mimaDefaultSettings

def ciSettings: Seq[Def.Setting[_]] =
  if (sys.env.getOrElse("TRAVIS", "false").toBoolean) Seq(
    ivyLoggingLevel := UpdateLogging.Quiet,
    logLevel in Global := Level.Warn,
    logLevel in Compile := Level.Warn,
    logLevel in Test := Level.Info
  ) else Seq.empty[Def.Setting[_]]

val testCleanup = Seq(
  testOptions in Test += Tests.Cleanup { loader =>
    val c = loader.loadClass("com.twitter.storehaus.testing.Cleanup$")
    c.getMethod("cleanup").invoke(c.getField("MODULE$").get(c))
  }
)

val ignoredABIProblems = {
  import com.typesafe.tools.mima.core._
  import com.typesafe.tools.mima.core.ProblemFilters._
  Seq(
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$Link"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$Link$"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka.JavaFutureToTwitterFutureConverter$"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka.JavaFutureToTwitterFutureConverter"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$Open"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$Open$"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$State"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka" +
      ".JavaFutureToTwitterFutureConverter$State$"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.kafka.KafkaStore.this"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.kafka.KafkaStore.apply"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.algebra.ReadableStoreSemigroup" +
      ".plus"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mongodb.MongoStore.getValue"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.asynchbase.AsyncHBaseByteArrayStore" +
      ".futurePool"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.asynchbase.AsyncHBaseLongStore" +
      ".futurePool"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.asynchbase.AsyncHBaseStringStore" +
      ".futurePool"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.asynchbase.AsyncHBaseStore.futurePool"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.ReadThroughStore.mutex"),
    exclude[MissingClassProblem]("com.twitter.storehaus.kafka.JavaFutureToTwitterFutureConverter$Closed$"),
    exclude[DirectMissingMethodProblem]("com.twitter.storehaus.kafka.KafkaStore.<init>$default$3"),
    exclude[MissingMethodProblem]("com.twitter.storehaus.cache.Cache.occupancy"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.http.HttpException.apply"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.apply"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.MySqlLongStore.apply"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.ValueMapper" +
      ".toChannelBuffer"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.ValueMapper.toLong"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.ValueMapper.toString"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlValue.v"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.MySqlValue.this"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.client"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.deleteStmt"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.updateStmt"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.selectStmt"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.this"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.storehaus.mysql.MySqlStore.insertStmt")
  )
}

val sharedSettings = extraSettings ++ ciSettings ++ Seq(
  organization := "com.twitter",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.10.6", scalaVersion.value),
  javacOptions ++= (
    if (scalaVersion.value.startsWith("2.10.")) Seq("-source", "1.6", "-target", "1.6")
    else Seq("-source", "1.8", "-target", "1.8")
  ),
  javacOptions in doc := (
    if (scalaVersion.value.startsWith("2.10.")) Seq("-source", "1.6")
    else Seq("-source", "1.8")
  ),
  libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  resolvers ++= Seq(
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases,
    "Conjars Repository" at "http://conjars.org/repo",
    // this repo is needed to retrieve the excluded dependencies from storehaus-memcache
    // during mima checks
    "Twitter Maven" at "http://maven.twttr.com"
  ),
  parallelExecution in Test := true,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Yresolve-term-conflict:package"
  ),

  // add linter for common scala issues: https://github.com/HairyFotr/linter
  addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.14"),

  // Publishing options:

  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseVersionBump := sbtrelease.Version.Bump.Minor, // need to tweak based on mima results
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { x => false },

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
 // runTest, // tests need services installed that travis has. MAKE SURE YOU ARE GREEN!!!
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges),

  publishTo := Some(
      if (version.value.trim.toUpperCase.endsWith("SNAPSHOT"))
        Opts.resolver.sonatypeSnapshots
      else Opts.resolver.sonatypeStaging
    ),
  pomExtra :=
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
    </developers>
)

/**
  * This returns the youngest jar we released that is compatible with
  * the current.
  */
val ignoredModules = Set[String]("benchmark", "elasticsearch")

def youngestForwardCompatible(subProj: String) =
  Some(subProj)
    .filterNot(ignoredModules.contains)
    .map { s => "com.twitter" %% s"storehaus-$s" % "0.15.0-RC1" }

lazy val noPublishSettings = Seq(
    publish := (),
    publishLocal := (),
    test := (),
    publishArtifact := false
  )

val algebirdVersion = "0.12.0"
val bijectionVersion = "0.9.1"
def utilVersion(scalaVersionValue: String) = if (scalaVersionValue.startsWith("2.10.")) "6.34.0" else "6.42.0"
val scaldingVersion = "0.16.0-RC1"
def finagleVersion(scalaVersionValue: String) = if (scalaVersionValue.startsWith("2.10.")) "6.35.0" else "6.43.0"
val scalatestVersion = "2.2.4"

lazy val storehaus = Project(
  id = "storehaus",
  base = file("."),
  settings = sharedSettings)
  .settings(noPublishSettings)
  .aggregate(
  storehausCache,
  storehausCore,
  storehausAlgebra,
  storehausMemcache,
  storehausMySQL,
  storehausRedis,
  storehausHBase,
  storehausDynamoDB,
  storehausLevelDB,
  storehausKafka,
  storehausMongoDB,
  storehausElastic,
  storehausHttp,
  storehausTesting,
  storehausBenchmark
)

def module(name: String) = {
  val id = "storehaus-%s".format(name)
  Project(id = id, base = file(id), settings = sharedSettings ++ testCleanup ++ Seq(
    Keys.name := id,
    mimaPreviousArtifacts := youngestForwardCompatible(name).toSet,
    mimaBinaryIssueFilters ++= ignoredABIProblems
  )
  ).dependsOn(storehausTesting % "test->test")
}

lazy val storehausCache = module("cache").settings(
  libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion,
  libraryDependencies += withCross("com.twitter" %% "util-core" % utilVersion(scalaVersion.value))
)

lazy val storehausCore = module("core").settings(
  libraryDependencies ++= Seq(
    withCross("com.twitter" %% "util-core" % utilVersion(scalaVersion.value) % "provided"),
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
    "com.twitter" %% "finagle-memcached" % finagleVersion(scalaVersion.value)
  )
).dependsOn(storehausAlgebra % "test->test;compile->compile")

lazy val storehausMySQL = module("mysql").settings(
  libraryDependencies += "com.twitter" %% "finagle-mysql" % finagleVersion(scalaVersion.value)
).dependsOn(storehausAlgebra % "test->test;compile->compile")

lazy val storehausRedis = module("redis").settings(
  libraryDependencies ++= Seq (
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "bijection-netty" % bijectionVersion,
    "com.twitter" %% "finagle-redis" % finagleVersion(scalaVersion.value)
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

lazy val storehausKafka = module("kafka").settings(
  libraryDependencies ++= Seq (
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "bijection-avro" % bijectionVersion,
    "org.apache.kafka" % "kafka-clients" % "0.9.0.1",
    "org.apache.zookeeper" % "zookeeper" % "3.4.8" % "test",
    "org.apache.kafka" %% "kafka" % "0.9.0.1" % "test"
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
    "com.google.code.findbugs" % "jsr305" % "1.3.9",
    "com.twitter" %% "bijection-json4s" % bijectionVersion,
    "org.slf4j" % "slf4j-api" % "1.7.21" % "test",
    "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test"
  ),
  // we don't want various tests clobbering each others keys
  parallelExecution in Test := false
).dependsOn(storehausAlgebra % "test->test;compile->compile")

lazy val storehausTesting = Project(
  id = "storehaus-testing",
  base = file("storehaus-testing"),
  settings = sharedSettings ++ Seq(
    name := "storehaus-testing",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.12.2" withSources(),
      withCross("com.twitter" %% "util-core" % utilVersion(scalaVersion.value))
    )
  )
)

lazy val storehausBenchmark = module("benchmark")
  .settings(JmhPlugin.projectSettings:_*)
  .settings(noPublishSettings)
  .settings(
    libraryDependencies ++= Seq(
        "com.twitter" %% "bijection-core" % bijectionVersion,
        "com.twitter" %% "algebird-core" % algebirdVersion
      ))
  .settings(coverageExcludedPackages := "com\\.twitter\\.storehaus\\.benchmark.*")
  .dependsOn(storehausCore, storehausAlgebra, storehausCache).enablePlugins(JmhPlugin)

lazy val storehausHttp = module("http").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "finagle-http" % finagleVersion(scalaVersion.value),
    "com.twitter" %% "bijection-netty" % bijectionVersion
  ) ++ (
    if (scalaVersion.value.startsWith("2.10."))
      Seq("com.twitter" %% "finagle-http-compat" % finagleVersion(scalaVersion.value))
    else
      Seq()
  )
).dependsOn(storehausCore)
