resolvers ++= Seq(
  "jgit-repo" at "https://download.eclipse.org/jgit/maven"
)

addSbtPlugin("com.eed3si9n"       % "sbt-assembly"    % "0.15.0")
addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"      % "0.4.3")
addSbtPlugin("com.47deg"          % "sbt-microsites"  % "1.2.1")
addSbtPlugin("com.github.gseitz"  % "sbt-release"     % "1.0.13")
addSbtPlugin("com.jsuereth"       % "sbt-pgp"         % "2.0.1")
addSbtPlugin("com.typesafe"       % "sbt-mima-plugin" % "0.7.0")
addSbtPlugin("com.typesafe.sbt"   % "sbt-ghpages"     % "0.6.3")
addSbtPlugin("org.scalariform"    % "sbt-scalariform" % "1.8.3")
addSbtPlugin("io.spray"           % "sbt-boilerplate" % "0.6.1")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "1.6.1")
addSbtPlugin("org.xerial.sbt"     % "sbt-sonatype"    % "3.9.4")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.3.7")
