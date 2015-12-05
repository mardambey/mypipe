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

lazy val snapshotterDependencies = Seq(
  logback,
  mysqlAsync,
  scalaTest,
  scopt,
  typesafeConfig
)

lazy val producersDependencies = Seq(
  akkaActor,
  typesafeConfig
)

lazy val avroDependencies = Seq(
  avro,
  guava,
  xinject,
  jerseyServer,
  jerseyServlet,
  jerseyCore,
  jsr305,
  rsApi,
  scalaTest,
  scalaReflect,
  schemaRepoBundle
)

lazy val kafkaDependencies = Seq(
  kafka,
  scalaTest,
  schemaRepoBundle
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "mypipe").
  aggregate(api, producers, runner, snapshotter, myavro, mykafka)

lazy val runner = (project in file("mypipe-runner")).
  settings(commonSettings: _*).
  settings(
    name := "runner",
    fork in run := false,
    libraryDependencies ++= runnerDependencies).
  settings(Format.settings) dependsOn(api, producers, myavro, mykafka)

lazy val snapshotter = (project in file("mypipe-snapshotter")).
  settings(commonSettings: _*).
  settings(
    name := "snapshotter",
    fork in run := false,
    libraryDependencies ++= snapshotterDependencies).
  settings(Format.settings) dependsOn(api % "compile->compile;test->test", producers, myavro, mykafka, runner)

lazy val producers = (project in file("mypipe-producers")).
  settings(commonSettings: _*).
  settings(
    name := "producers",
    fork in run := false,
    libraryDependencies ++= producersDependencies).
  settings(Format.settings) dependsOn(api)

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

lazy val mykafka = (project in file("mypipe-kafka")).
  settings(commonSettings: _*).
  settings(
    name := "mykafka",
    fork in run := false,
    libraryDependencies ++= kafkaDependencies,
    parallelExecution in Test := false).
  settings(AvroCompiler.settingsTest).
  settings(Format.settings) dependsOn(api % "compile->compile;test->test", myavro)

