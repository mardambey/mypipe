import Dependencies._

lazy val commonSettings = Seq(
  name := "mypipe-asana-fork",
  version := "1.0.4",
  organization := "com.github.asana",
  // crossScalaVersions := Seq("2.12.14", "2.13.6"), // The dream, but https://github.com/mauricio/postgresql-async is not 2.13 compatible
  crossScalaVersions := Seq("2.12.14"),
  exportJars := true,
  ThisBuild / parallelExecution := false,

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value
  ),
  resolvers ++= Seq(Resolver.mavenLocal,
    "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Asana Repository" at "https://dl.bintray.com/asana/maven",
    "Twitter Repository" at "https://maven.twttr.com/")
)

lazy val apiDependencies = Seq(
  akkaActor,
  akkaAgent,
  commonsLang,
  jsqlParser,
  jug,
  logback,
  mysqlAsync,
  mysqlBinlogConnectorJava,
  scalaTest,
  slf4jApi,
  typesafeConfig
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    publish / skip := true
  ).
  aggregate(api)

lazy val api = (project in file("mypipe-api")).
  settings(commonSettings: _*).
  settings(
    name := "mypipe-api-asana-fork",
    run / fork := false,
    libraryDependencies ++= apiDependencies,
    Test / parallelExecution := false,
    licenses := List(
        ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
      ),
    homepage := Some(url("https://github.com/Asana/mypipe-asana-fork")),
    publishMavenStyle := true,
    publishTo := Some("Asana Maven" at "s3://asana-oss-cache/maven/release"),
    Test / publishArtifact := false,
  )
