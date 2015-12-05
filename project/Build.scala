import sbt._
import Keys._

object Dependencies {
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.5"
  val akkaAgent = "com.typesafe.akka" %% "akka-agent" % "2.3.5"
  val avro = "org.apache.avro" % "avro" % "1.7.7"
  val commonsLang = "commons-lang" % "commons-lang" % "2.6"
  val guava = "com.google.guava" % "guava" % "14.0.1"
  val jsqlParser = "com.github.jsqlparser" % "jsqlparser" % "0.9"
  val jsr305 = "com.google.code.findbugs" % "jsr305" % "1.3.+"
  val jug = "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.3"
  val jerseyServer = "com.sun.jersey" % "jersey-server" % "1.15"
  val jerseyServlet = "com.sun.jersey" % "jersey-servlet" % "1.15"
  val jerseyCore = "com.sun.jersey" % "jersey-core" % "1.15"
  val kafka = "org.apache.kafka" % "kafka_2.11" % "0.8.2.2" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
  val logback = "ch.qos.logback" % "logback-classic" % "1.1.1"
  val mysqlAsync =  "com.github.mauricio" % "mysql-async_2.11" % "0.2.18"
  val mysqlBinlogConnectorJava = "com.github.shyiko" % "mysql-binlog-connector-java" % "0.2.4"
  val rsApi = "javax.ws.rs" % "javax.ws.rs-api" % "2.0.1"
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.2"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.2"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.1" % "test"
  val schemaRepoClient = "org.schemarepo" % "schema-repo-client" % "0.1.3"
  val schemaRepoServer = "org.schemarepo" % "schema-repo-server" % "0.1.3"
  val schemaRepoBundle = "org.schemarepo" % "schema-repo-bundle" % "0.1.3"
  val scopt = "com.github.scopt" %% "scopt" % "3.3.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.2.1"
  val xinject = "javax.inject" % "javax.inject" % "1"
}

object AvroCompiler {
  import sbtavro.SbtAvro._

  lazy val settingsTest = avroSettings ++ Seq(
    sourceDirectory in avroConfig <<= (sourceDirectory in Test)(_ / "avro"),
    version in avroConfig := "1.7.7"
  )

  lazy val settingsCompile = avroSettings ++ Seq(
    version in avroConfig := "1.7.7"
  )
}

object Format {
  import com.typesafe.sbt.SbtScalariform._

  lazy val settings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences := formattingPreferences
  )

  lazy val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().
      setPreference(AlignParameters, true).
      setPreference(AlignSingleLineCaseStatements, true).
      setPreference(DoubleIndentClassDeclaration, true).
      setPreference(IndentLocalDefs, true).
      setPreference(IndentPackageBlocks, true).
      setPreference(IndentSpaces, 2).
      setPreference(MultilineScaladocCommentsStartOnFirstLine, true).
      setPreference(PreserveSpaceBeforeArguments, false).
      setPreference(PreserveDanglingCloseParenthesis, false).
      setPreference(RewriteArrowSymbols, true).
      setPreference(SpaceBeforeColon, false).
      setPreference(SpaceInsideBrackets, false).
      setPreference(SpacesWithinPatternBinders, true)
  }
}
