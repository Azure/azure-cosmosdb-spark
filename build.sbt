organization := "com.microsoft.azure"
name := "azure-cosmosdb-spark"
description := "Spark Connector for Microsoft Azure CosmosDB."

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature"
)

val sparkVersion = "2.1.0"
val slf4jVersion = "1.7.6"
val log4jVersion = "1.2.17"
val tinkerpopVersion = "3.2.5"

libraryDependencies ++= Seq(
  "com.microsoft.azure" % "azure-documentdb" % "1.12.0",
  "com.microsoft.azure" % "azure-documentdb-rx" % "0.9.0-rc1",

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-launcher" % sparkVersion,
  "org.apache.spark" %% "spark-network-common" % sparkVersion,
  "org.apache.spark" %% "spark-network-shuffle" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-unsafe" % sparkVersion,

  "io.reactivex" % "rxjava" % "1.3.0",

  "org.slf4j" % "slf4j-api" % slf4jVersion,

  "org.apache.tinkerpop" % "spark-gremlin" % tinkerpopVersion excludeAll(
//    ExclusionRule(organization = "com.fasterxml.jackson.core"),
//    ExclusionRule(organization = "com.thoughtworks.paranamer"),
//    ExclusionRule(organization = "commons-lang"),
    ExclusionRule(organization = "org.apache.spark"),
    ExclusionRule(organization = "org.scala-lang")
  ),
  "org.apache.tinkerpop" % "tinkergraph-gremlin" % tinkerpopVersion,

  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
).map(_ % "compile")

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % "test",
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

