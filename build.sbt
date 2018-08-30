name := "evolving_wikipedia_graph"

version := "0.1"

scalaVersion := "2.11.12"

// options for log4j
fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1" %"provided",

  "org.apache.spark" %% "spark-sql" % "2.3.1" %"provided",

  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.21",

  "info.bliki.wiki" % "bliki-core" % "3.1.0",

  "com.typesafe" % "config" % "1.3.2",

  "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
)

// Resolve dependency conflict for SLF4J
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}