import sbt._
import Keys._

object Dependencies {

  val guava = "com.google.guava" % "guava" % "14.0.1"
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.3.5"
  val akkaAgent = "com.typesafe.akka" %% "akka-agent" % "2.3.5"
  val avro = "org.apache.avro" % "avro" % "1.7.5"
  val commonsLang = "commons-lang" % "commons-lang" % "2.6"
  val jsqlParser = "com.github.jsqlparser" % "jsqlparser" % "0.9"
  val jsr305 = "com.google.code.findbugs" % "jsr305" % "1.3.+"
  val jug = "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.3"
  val kafka = "org.apache.kafka" % "kafka_2.11" % "0.8.2-beta" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
  val logback = "ch.qos.logback" % "logback-classic" % "1.1.1"
  val mysqlAsync =  "com.github.mauricio" % "mysql-async_2.11" % "0.2.15"
  val mysqlBinlogConnectorJava = "com.github.shyiko" % "mysql-binlog-connector-java" % "0.2.2"
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.2"
  val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.11.2"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.1" % "test"
  val typesafeConfig = "com.typesafe" % "config" % "1.2.1"
  val xinject = "javax.inject" % "javax.inject" % "1"
  val redisclient = "net.debasishg" %% "redisclient" % "3.0"
}

object AvroCompiler {
  import sbtavro.SbtAvro._

  lazy val settingsTest = avroSettings ++ Seq(
    sourceDirectory in avroConfig <<= (sourceDirectory in Test)(_ / "avro"),
    version in avroConfig := "1.7.5"
  )

  lazy val settingsCompile = avroSettings ++ Seq(
    version in avroConfig := "1.7.5"
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
