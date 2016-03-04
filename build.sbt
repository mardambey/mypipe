import Dependencies._

lazy val commonSettings = Seq(
  name := "mypipe",
  version := "0.0.1",
  organization := "mypipe",
  scalaVersion := "2.11.7",
  exportJars := true,
  parallelExecution in ThisBuild := false,
  resolvers ++= Seq(Resolver.mavenLocal,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Twitter Repository" at "http://maven.twttr.com/")
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
  scalaCompiler,
  scalaReflect,
  scalaTest,
  typesafeConfig
)

lazy val runnerDependencies = Seq(
  akkaActor,
  typesafeConfig,
  redisclient,
  sqsclient,
  scalaTest,
  play
)

lazy val mypipe = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "mypipe",
    fork in run := false,
    libraryDependencies ++= runnerDependencies,
    parallelExecution in Test := false).
  settings(Format.settings) dependsOn(api % "compile->compile;test->test")

lazy val api = (project in file("mypipe-api")).
  settings(commonSettings: _*).
  settings(
    name := "api",
    fork in run := false,
    libraryDependencies ++= apiDependencies,
    parallelExecution in Test := false).
  settings(Format.settings)
