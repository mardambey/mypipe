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
  typesafeConfig
)

lazy val producersDependencies = Seq(
  akkaActor,
  typesafeConfig,
  redisclient,
  sqsclient,
  scalaTest
)

lazy val avroDependencies = Seq(
  avro,
  guava,
  xinject,
  jsr305,
  scalaTest,
  scalaReflect
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "mypipe").
  aggregate(api, producers, runner, myavro)

lazy val runner = (project in file("mypipe-runner")).
  settings(commonSettings: _*).
  settings(
    name := "runner",
    fork in run := false,
    libraryDependencies ++= runnerDependencies).
  settings(Format.settings) dependsOn(api, producers, myavro)

lazy val producers = (project in file("mypipe-producers")).
  settings(commonSettings: _*).
  settings(
    name := "producers",
    fork in run := false,
    libraryDependencies ++= producersDependencies,
    parallelExecution in Test := false).
  settings(AvroCompiler.settingsTest).
  settings(Format.settings) dependsOn(api % "compile->compile;test->test", myavro)

lazy val api = (project in file("mypipe-api")).
  settings(commonSettings: _*).
  settings(
    name := "api",
    fork in run := false,
    libraryDependencies ++= apiDependencies,
    parallelExecution in Test := false).
  settings(Format.settings)

lazy val myavro = (project in file("mypipe-avro")).
  settings(commonSettings: _*).
  settings(
    name := "myavro",
    fork in run := false,
    libraryDependencies ++= avroDependencies,
    parallelExecution in Test := false).
  settings(AvroCompiler.settingsCompile).
  settings(Format.settings) dependsOn(api % "compile->compile;test->test")
