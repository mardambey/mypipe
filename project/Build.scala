import sbt._
import Keys._

object ApplicationBuild extends Build {

  override lazy val settings = super.settings ++
    Seq(
      name := "mypipe",
      version := "0.0.1",
      organization := "mypipe",
      scalaVersion := "2.10.3",
      parallelExecution in ThisBuild := false,
      resolvers ++= Seq(Resolver.mavenLocal,
        "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
        "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
        "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
        "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
        "Twitter Repository" at "http://maven.twttr.com/")
    )

  import Dependencies._

  val apiDependencies = Seq(
    akkaActor,
    akkaAgent,
    commonsLang,
    mysqlAsync,
    mysqlBinlogConnectorJava,
    scalaTest,
    typesafeConfig,
    logback
  )

  val samplesDependencies = Seq(
    commonsLang,
    astyanaxCassansra,
    astyanaxCore,
    astyanaxThrift,
    mysqlAsync,
    mysqlBinlogConnectorJava
  )

  val runnerDependencies = Seq(
    typesafeConfig
  )

  val producersDependencies = Seq(
    akkaActor,
    typesafeConfig
  )

  val avroDependencies = Seq(
    avro,
		guava,
    xinject,
    jsr305,
    scalaTest,
    scalaReflect
  )

  val kafkaDependencies = Seq(
    kafka,
    scalaTest
  )

  val cassandraDependencies = Seq(
    astyanaxCassansra,
    astyanaxCore,
    astyanaxThrift
  )

  lazy val root = Project(id = "mypipe",
    base = file(".")) aggregate(api, producers, runner, myavro, mykafka)

  lazy val runner = Project(id = "runner",
    base = file("mypipe-runner"),
    settings = Project.defaultSettings ++ Seq(
      fork in run := false,
      libraryDependencies ++= runnerDependencies
    ) ++ Format.settings
  ) dependsOn(api, producers, myavro, mykafka)


  lazy val producers = Project(id = "producers",
    base = file("mypipe-producers"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= producersDependencies
    ) ++ Format.settings
  ) dependsOn (api)

  lazy val api = Project(id = "api",
    base = file("mypipe-api"),
    settings = Project.defaultSettings ++ Seq(
      parallelExecution in Test := false,
      libraryDependencies ++= apiDependencies
    ) ++ Format.settings
  )

  lazy val samples = Project(id = "samples",
    base = file("mypipe-samples"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= samplesDependencies
    ) ++ Format.settings
  ) dependsOn(api, producers, mycassandra)

  lazy val myavro = Project(id = "myavro",
    base = file("mypipe-avro"),
    settings = Project.defaultSettings ++ Seq(
      parallelExecution in Test := false,
      libraryDependencies ++= avroDependencies
    ) ++ Format.settings
  ) dependsOn(api % "compile->compile;test->test")

  lazy val mykafka = Project(id = "mykafka",
    base = file("mypipe-kafka"),
    settings = Project.defaultSettings ++ Seq(
      parallelExecution in Test := false,
      libraryDependencies ++= kafkaDependencies
    ) ++ Format.settings
  ) dependsOn(api % "compile->compile;test->test", myavro)

  lazy val mycassandra = Project(id = "mycassandra",
    base = file("mypipe-cassandra"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= cassandraDependencies
    ) ++ Format.settings
  ) dependsOn(api)
}

object Dependencies {

    val scalaTest = "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
    val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.2.3"
    val akkaAgent = "com.typesafe.akka" %% "akka-agent" % "2.2.3"
    val typesafeConfig = "com.typesafe" % "config" % "1.2.0"
    val mysqlBinlogConnectorJava = "com.github.shyiko" % "mysql-binlog-connector-java" % "0.1.0-SNAPSHOT"
    val commonsLang = "commons-lang" % "commons-lang" % "2.6"
    val mysqlAsync = "com.github.mauricio" %% "mysql-async" % "0.2.12"
    val astyanaxCore = "com.netflix.astyanax" % "astyanax-core" % "1.56.48"
    val astyanaxThrift = "com.netflix.astyanax" % "astyanax-thrift" % "1.56.48"
    val astyanaxCassansra = "com.netflix.astyanax" % "astyanax-cassandra" % "1.56.48"
    val avro = "org.apache.avro" % "avro" % "1.7.5"
    val kafka = "org.apache.kafka" %% "kafka" % "0.8.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
		val guava = "com.google.guava" % "guava" % "14.0.1"
    val xinject = "javax.inject" % "javax.inject" % "1"
    val jsr305 = "com.google.code.findbugs" % "jsr305" % "1.3.+"
    val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.10.3"
    val logback = "ch.qos.logback" % "logback-classic" % "1.1.1"
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

