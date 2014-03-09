import sbt._
import Keys._

object ApplicationBuild extends Build {
  override lazy val settings = super.settings ++
    Seq(
      name := "mypipe",
      version := "0.0.1",
      organization := "mypipe",
      scalaVersion := "2.10.3",
      fork in run := true,
      parallelExecution in Test := false,
      resolvers ++= Seq(Resolver.mavenLocal,
        "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
        "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
        "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
        "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
        "Twitter Repository" at "http://maven.twttr.com/")
    )

  val apiDependencies = Seq(
    "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
    "com.typesafe.akka" %% "akka-actor" % "2.2.3",
		"com.github.shyiko" % "mysql-binlog-connector-java" % "0.1.0-SNAPSHOT",
    "commons-lang" % "commons-lang" % "2.6",
    "com.netflix.astyanax" % "astyanax-core" % "1.56.48",
    "com.netflix.astyanax" % "astyanax-thrift" % "1.56.48",
    "com.netflix.astyanax" % "astyanax-cassandra" % "1.56.48",
		"com.github.mauricio" %% "mysql-async" % "0.2.12"
  )

	val samplesDependencies = apiDependencies

  val runnerDependencies = apiDependencies

  lazy val root = Project(id = "mypipe",
    base = file(".")) aggregate (api, samples, runner)

  lazy val runner = Project(id = "runner",
    base = file("mypipe-runner"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= runnerDependencies
    ) ++ Format.settings
  ) dependsOn(api, samples)

  lazy val api = Project(id = "api",
    base = file("mypipe-api"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= apiDependencies
    ) ++ Format.settings
  )

  lazy val samples = Project(id = "samples",
    base = file("mypipe-samples"),
    settings = Project.defaultSettings ++ Seq(
      libraryDependencies ++= samplesDependencies
    ) ++ Format.settings
  ) dependsOn(api)
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

