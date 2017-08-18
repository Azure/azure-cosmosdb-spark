organization := "com.github.catalystcode"
name := "azure-cosmosdb-spark-samples"
description := "Scala examples of Cosmos DB on Spark that can serve as starting points for Cosmos DB-based Spark projects."

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature"
)

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "com.microsoft.azure" % "azure-documentdb" % "1.12.0",
  "com.microsoft.azure" % "azure-documentdb-rx" % "0.9.0-rc1",
  "com.microsoft.azure" % "azure-cosmosdb-spark_2.2.0_2.11" % "0.0.3" intransitive(),

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
).map(_ % "compile")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

