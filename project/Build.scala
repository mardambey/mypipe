import sbt._
import Keys._

object Dependencies {
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.5.23"
  val akkaAgent = "com.typesafe.akka" %% "akka-agent" % "2.5.23"
  val avro = "org.apache.avro" % "avro" % "1.7.7"
  val commonsLang = "commons-lang" % "commons-lang" % "2.6" force()
  val guava = "com.google.guava" % "guava" % "19.0"
  val jsqlParser = "com.github.jsqlparser" % "jsqlparser" % "0.9"
  val jsr305 = "com.google.code.findbugs" % "jsr305" % "1.3.+"
  val jug = "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  val jerseyServlet = "com.sun.jersey" % "jersey-servlet" % "1.15"
  val jerseyCore = "com.sun.jersey" % "jersey-core" % "1.15"
  val kafka = "org.apache.kafka" % "kafka_2.11" % "0.8.2.2" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("org.slf4j", "slf4j-log4j12")
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" force()
  val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.25" force()
  val mysqlAsync =  "com.github.mauricio" %% "mysql-async" % "0.2.21"
  // val mysqlBinlogConnectorJava = "com.github.shyiko" % "mysql-binlog-connector-java" % "0.2.4"
//  val mysqlBinlogConnectorJava = "com.github.asana" % "mysql-binlog-connector-java-asana-fork" % "0.2.3"
  val rsApi = "javax.ws.rs" % "javax.ws.rs-api" % "2.0.1"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  val schemaRepoClient = "org.schemarepo" % "schema-repo-client" % "0.1.3"
  val schemaRepoServer = "org.schemarepo" % "schema-repo-server" % "0.1.3"
  val schemaRepoBundle = "org.schemarepo" % "schema-repo-bundle" % "0.1.3"
  val scopt = "com.github.scopt" %% "scopt" % "3.3.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.3"
  val xinject = "javax.inject" % "javax.inject" % "1"
}
