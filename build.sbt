name := "evolving_wikipedia_graph"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1" %"provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1" %"provided",
  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.21",
  "info.bliki.wiki" % "bliki-core" % "3.1.0"
)

// Resolve dependency conflict for SLF4J
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}