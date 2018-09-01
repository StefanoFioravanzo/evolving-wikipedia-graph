name := "evolving_wikipedia_graph"

version := "0.1"

scalaVersion := "2.11.12"

fork in run := true

lazy val sparkDep = Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1"
//  "org.apache.spark" %% "spark-sql" % "2.3.1"
)

lazy val otherDeps = Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.5",

  "org.apache.logging.log4j" % "log4j-api" % "2.5",

  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.21",

  "info.bliki.wiki" % "bliki-core" % "3.1.0",

  "com.typesafe" % "config" % "1.3.2",

  "joda-time" % "joda-time" % "2.10"
)

libraryDependencies ++= sparkDep.map(_ % "provided")
libraryDependencies ++= otherDeps
// Resolve dependency conflict for SLF4J
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

// use this to make sure the "provided" dependencies are included at runtime (for local development)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
