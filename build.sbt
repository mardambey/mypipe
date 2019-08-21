import Dependencies._

lazy val commonSettings = Seq(
  name := "mypipe-asana-fork",
  version := "1.0.2",
  organization := "com.github.asana",
  crossScalaVersions := Seq("2.11.12", "2.12.8"),
  exportJars := true,
  parallelExecution in ThisBuild := false,

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value
  ),
  resolvers ++= Seq(Resolver.mavenLocal,
    "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
    "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Asana Repository" at "https://dl.bintray.com/asana/maven",
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
  scalaTest,
  slf4jApi,
  typesafeConfig
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    skip in publish := true
  ).
  aggregate(api)

lazy val api = (project in file("mypipe-api")).
  settings(commonSettings: _*).
  settings(
    name := "mypipe-api-asana-fork",
    fork in run := false,
    libraryDependencies ++= apiDependencies,
    parallelExecution in Test := false,
    licenses := List(
        ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
      ),
    homepage := Some(url("https://github.com/Asana/mypipe-asana-fork")),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    bintrayOrganization := Some("asana"),
    bintrayRepository := "maven",
    bintrayPackage := "mypipe-asana-fork",
    bintrayReleaseOnPublish in ThisBuild := true
  ).
  settings(Format.settings)

// lazy val runnerDependencies = Seq(
//   typesafeConfig
// )
// 
// lazy val snapshotterDependencies = Seq(
//   logback,
//   mysqlAsync,
//   scalaTest,
//   scopt,
//   typesafeConfig
// )
// 
// lazy val producersDependencies = Seq(
//   akkaActor,
//   typesafeConfig
// )
// 
// lazy val avroDependencies = Seq(
//   avro,
//   guava,
//   xinject,
//   jerseyServlet,
//   jerseyCore,
//   jsr305,
//   rsApi,
//   scalaTest,
//   scalaReflect,
//   schemaRepoBundle
// )
// 
// lazy val kafkaDependencies = Seq(
//   kafka,
//   scalaTest,
//   schemaRepoBundle
// )
//  aggregate(api, producers, runner, snapshotter, myavro, mykafka)

// lazy val runner = (project in file("mypipe-runner")).
//   settings(commonSettings: _*).
//   settings(
//     name := "runner",
//     fork in run := false,
//     libraryDependencies ++= runnerDependencies).
//   settings(Format.settings) dependsOn(api, producers, myavro, mykafka)
// 
// lazy val snapshotter = (project in file("mypipe-snapshotter")).
//   settings(commonSettings: _*).
//   settings(
//     name := "snapshotter",
//     fork in run := false,
//     libraryDependencies ++= snapshotterDependencies).
//   settings(Format.settings) dependsOn(api % "compile->compile;test->test", producers, myavro, mykafka, runner)
// 
// lazy val producers = (project in file("mypipe-producers")).
//   settings(commonSettings: _*).
//   settings(
//     name := "producers",
//     fork in run := false,
//     libraryDependencies ++= producersDependencies).
//   settings(Format.settings) dependsOn(api)


// lazy val myavro = (project in file("mypipe-avro")).
//   settings(commonSettings: _*).
//   settings(
//     name := "myavro",
//     fork in run := false,
//     libraryDependencies ++= avroDependencies,
//     parallelExecution in Test := false).
//   settings(AvroCompiler.settingsCompile).
//   settings(Format.settings) dependsOn(api % "compile->compile;test->test")
// 
// lazy val mykafka = (project in file("mypipe-kafka")).
//   settings(commonSettings: _*).
//   settings(
//     name := "mykafka",
//     fork in run := false,
//     libraryDependencies ++= kafkaDependencies,
//     parallelExecution in Test := false).
//   settings(AvroCompiler.settingsTest).
//   settings(Format.settings) dependsOn(api % "compile->compile;test->test", myavro)

